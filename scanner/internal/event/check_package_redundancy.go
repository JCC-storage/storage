package event

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc"
	lrcparser "gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc/parser"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/config"
)

type CheckPackageRedundancy struct {
	*scevt.CheckPackageRedundancy
}

func NewCheckPackageRedundancy(evt *scevt.CheckPackageRedundancy) *CheckPackageRedundancy {
	return &CheckPackageRedundancy{
		CheckPackageRedundancy: evt,
	}
}

type StorageLoadInfo struct {
	Storage      stgmod.StorageDetail
	AccessAmount float64
}

func (t *CheckPackageRedundancy) TryMerge(other Event) bool {
	event, ok := other.(*CheckPackageRedundancy)
	if !ok {
		return false
	}

	return event.PackageID == t.PackageID
}

func (t *CheckPackageRedundancy) Execute(execCtx ExecuteContext) {
	log := logger.WithType[CheckPackageRedundancy]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.CheckPackageRedundancy))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	// TODO 应该像其他event一样直接读取数据库

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		log.Warnf("new coordinator client: %s", err.Error())
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getObjs, err := coorCli.GetPackageObjectDetails(coormq.ReqGetPackageObjectDetails(t.PackageID))
	if err != nil {
		log.Warnf("getting package objects: %s", err.Error())
		return
	}

	stats, err := execCtx.Args.DB.PackageAccessStat().GetByPackageID(execCtx.Args.DB.DefCtx(), t.PackageID)
	if err != nil {
		log.Warnf("getting package access stats: %s", err.Error())
		return
	}

	// TODO UserID
	getStgs, err := coorCli.GetUserStorageDetails(coormq.ReqGetUserStorageDetails(1))
	if err != nil {
		log.Warnf("getting all storages: %s", err.Error())
		return
	}

	if len(getStgs.Storages) == 0 {
		log.Warnf("no available storages")
		return
	}

	userAllStorages := make(map[cdssdk.StorageID]*StorageLoadInfo)
	for _, stg := range getStgs.Storages {
		userAllStorages[stg.Storage.StorageID] = &StorageLoadInfo{
			Storage: stg,
		}
	}

	for _, stat := range stats {
		info, ok := userAllStorages[stat.StorageID]
		if !ok {
			continue
		}
		info.AccessAmount = stat.Amount
	}

	var changedObjects []coormq.UpdatingObjectRedundancy

	defRep := cdssdk.DefaultRepRedundancy
	defEC := cdssdk.DefaultECRedundancy

	// TODO 目前rep的备份数量固定为2，所以这里直接选出两个节点
	// TODO 放到chooseRedundancy函数中
	mostBlockStgIDs := t.summaryRepObjectBlockStorages(getObjs.Objects, 2)
	newRepStgs := t.chooseNewStoragesForRep(&defRep, userAllStorages)
	rechoosedRepStgs := t.rechooseStoragesForRep(mostBlockStgIDs, &defRep, userAllStorages)
	newECStgs := t.chooseNewStoragesForEC(&defEC, userAllStorages)

	// 加锁
	builder := reqbuilder.NewBuilder()
	for _, storage := range newRepStgs {
		builder.Shard().Buzy(storage.Storage.Storage.StorageID)
	}
	for _, storage := range newECStgs {
		builder.Shard().Buzy(storage.Storage.Storage.StorageID)
	}
	mutex, err := builder.MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquiring dist lock: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	for _, obj := range getObjs.Objects {
		var updating *coormq.UpdatingObjectRedundancy
		var err error

		newRed, selectedStorages := t.chooseRedundancy(obj, userAllStorages)

		switch srcRed := obj.Object.Redundancy.(type) {
		case *cdssdk.NoneRedundancy:
			switch newRed := newRed.(type) {
			case *cdssdk.RepRedundancy:
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: none -> rep")
				updating, err = t.noneToRep(execCtx, obj, newRed, newRepStgs, userAllStorages)

			case *cdssdk.ECRedundancy:
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: none -> ec")
				updating, err = t.noneToEC(execCtx, obj, newRed, newECStgs, userAllStorages)

			case *cdssdk.LRCRedundancy:
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: none -> lrc")
				updating, err = t.noneToLRC(execCtx, obj, newRed, selectedStorages, userAllStorages)

			case *cdssdk.SegmentRedundancy:
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: none -> segment")
				updating, err = t.noneToSeg(execCtx, obj, newRed, selectedStorages, userAllStorages)
			}

		case *cdssdk.RepRedundancy:
			switch newRed := newRed.(type) {
			case *cdssdk.RepRedundancy:
				updating, err = t.repToRep(execCtx, obj, srcRed, rechoosedRepStgs, userAllStorages)

			case *cdssdk.ECRedundancy:
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: rep -> ec")
				updating, err = t.repToEC(execCtx, obj, newRed, newECStgs, userAllStorages)
			}

		case *cdssdk.ECRedundancy:
			switch newRed := newRed.(type) {
			case *cdssdk.RepRedundancy:
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: ec -> rep")
				updating, err = t.ecToRep(execCtx, obj, srcRed, newRed, newRepStgs, userAllStorages)

			case *cdssdk.ECRedundancy:
				uploadStorages := t.rechooseStoragesForEC(obj, srcRed, userAllStorages)
				updating, err = t.ecToEC(execCtx, obj, srcRed, newRed, uploadStorages, userAllStorages)
			}

		case *cdssdk.LRCRedundancy:
			switch newRed := newRed.(type) {
			case *cdssdk.LRCRedundancy:
				uploadStorages := t.rechooseStoragesForLRC(obj, srcRed, userAllStorages)
				updating, err = t.lrcToLRC(execCtx, obj, srcRed, newRed, uploadStorages, userAllStorages)
			}
		}

		if updating != nil {
			changedObjects = append(changedObjects, *updating)
		}

		if err != nil {
			log.WithField("ObjectID", obj.Object.ObjectID).Warnf("%s, its redundancy wont be changed", err.Error())
		}
	}

	if len(changedObjects) == 0 {
		return
	}

	_, err = coorCli.UpdateObjectRedundancy(coormq.ReqUpdateObjectRedundancy(changedObjects))
	if err != nil {
		log.Warnf("requesting to change object redundancy: %s", err.Error())
		return
	}
}

func (t *CheckPackageRedundancy) chooseRedundancy(obj stgmod.ObjectDetail, userAllStgs map[cdssdk.StorageID]*StorageLoadInfo) (cdssdk.Redundancy, []*StorageLoadInfo) {
	switch obj.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		if obj.Object.Size > config.Cfg().ECFileSizeThreshold {
			newStgs := t.chooseNewStoragesForEC(&cdssdk.DefaultECRedundancy, userAllStgs)
			return &cdssdk.DefaultECRedundancy, newStgs
		}

		return &cdssdk.DefaultRepRedundancy, t.chooseNewStoragesForRep(&cdssdk.DefaultRepRedundancy, userAllStgs)

	case *cdssdk.RepRedundancy:
		if obj.Object.Size > config.Cfg().ECFileSizeThreshold {
			newStgs := t.chooseNewStoragesForEC(&cdssdk.DefaultECRedundancy, userAllStgs)
			return &cdssdk.DefaultECRedundancy, newStgs
		}

	case *cdssdk.ECRedundancy:
		if obj.Object.Size <= config.Cfg().ECFileSizeThreshold {
			return &cdssdk.DefaultRepRedundancy, t.chooseNewStoragesForRep(&cdssdk.DefaultRepRedundancy, userAllStgs)
		}

	case *cdssdk.LRCRedundancy:
		newLRCStgs := t.rechooseStoragesForLRC(obj, &cdssdk.DefaultLRCRedundancy, userAllStgs)
		return &cdssdk.DefaultLRCRedundancy, newLRCStgs

	}
	return nil, nil
}

// 统计每个对象块所在的节点，选出块最多的不超过storageCnt个节点
func (t *CheckPackageRedundancy) summaryRepObjectBlockStorages(objs []stgmod.ObjectDetail, storageCnt int) []cdssdk.StorageID {
	type stgBlocks struct {
		StorageID cdssdk.StorageID
		Count     int
	}

	stgBlocksMap := make(map[cdssdk.StorageID]*stgBlocks)
	for _, obj := range objs {
		shouldUseEC := obj.Object.Size > config.Cfg().ECFileSizeThreshold
		if _, ok := obj.Object.Redundancy.(*cdssdk.RepRedundancy); ok && !shouldUseEC {
			for _, block := range obj.Blocks {
				if _, ok := stgBlocksMap[block.StorageID]; !ok {
					stgBlocksMap[block.StorageID] = &stgBlocks{
						StorageID: block.StorageID,
						Count:     0,
					}
				}
				stgBlocksMap[block.StorageID].Count++
			}
		}
	}

	storages := lo.Values(stgBlocksMap)
	sort2.Sort(storages, func(left *stgBlocks, right *stgBlocks) int {
		return right.Count - left.Count
	})

	ids := lo.Map(storages, func(item *stgBlocks, idx int) cdssdk.StorageID { return item.StorageID })
	if len(ids) > storageCnt {
		ids = ids[:storageCnt]
	}
	return ids
}

func (t *CheckPackageRedundancy) chooseNewStoragesForRep(red *cdssdk.RepRedundancy, allStgs map[cdssdk.StorageID]*StorageLoadInfo) []*StorageLoadInfo {
	sortedStorages := sort2.Sort(lo.Values(allStgs), func(left *StorageLoadInfo, right *StorageLoadInfo) int {
		return sort2.Cmp(right.AccessAmount, left.AccessAmount)
	})

	return t.chooseSoManyStorages(red.RepCount, sortedStorages)
}

func (t *CheckPackageRedundancy) chooseNewStoragesForEC(red *cdssdk.ECRedundancy, allStgs map[cdssdk.StorageID]*StorageLoadInfo) []*StorageLoadInfo {
	sortedStorages := sort2.Sort(lo.Values(allStgs), func(left *StorageLoadInfo, right *StorageLoadInfo) int {
		return sort2.Cmp(right.AccessAmount, left.AccessAmount)
	})

	return t.chooseSoManyStorages(red.N, sortedStorages)
}

func (t *CheckPackageRedundancy) chooseNewStoragesForLRC(red *cdssdk.LRCRedundancy, allStorages map[cdssdk.HubID]*StorageLoadInfo) []*StorageLoadInfo {
	sortedStorages := sort2.Sort(lo.Values(allStorages), func(left *StorageLoadInfo, right *StorageLoadInfo) int {
		return sort2.Cmp(right.AccessAmount, left.AccessAmount)
	})

	return t.chooseSoManyStorages(red.N, sortedStorages)
}

func (t *CheckPackageRedundancy) chooseNewStoragesForSeg(segCount int, allStgs map[cdssdk.StorageID]*StorageLoadInfo) []*StorageLoadInfo {
	sortedStorages := sort2.Sort(lo.Values(allStgs), func(left *StorageLoadInfo, right *StorageLoadInfo) int {
		return sort2.Cmp(right.AccessAmount, left.AccessAmount)
	})

	return t.chooseSoManyStorages(segCount, sortedStorages)
}

func (t *CheckPackageRedundancy) rechooseStoragesForRep(mostBlockStgIDs []cdssdk.StorageID, red *cdssdk.RepRedundancy, allStgs map[cdssdk.StorageID]*StorageLoadInfo) []*StorageLoadInfo {
	type rechooseStorage struct {
		*StorageLoadInfo
		HasBlock bool
	}

	var rechooseStgs []*rechooseStorage
	for _, stg := range allStgs {
		hasBlock := false
		for _, id := range mostBlockStgIDs {
			if id == stg.Storage.Storage.StorageID {
				hasBlock = true
				break
			}
		}

		rechooseStgs = append(rechooseStgs, &rechooseStorage{
			StorageLoadInfo: stg,
			HasBlock:        hasBlock,
		})
	}

	sortedStgs := sort2.Sort(rechooseStgs, func(left *rechooseStorage, right *rechooseStorage) int {
		// 已经缓存了文件块的节点优先选择
		v := sort2.CmpBool(right.HasBlock, left.HasBlock)
		if v != 0 {
			return v
		}

		return sort2.Cmp(right.AccessAmount, left.AccessAmount)
	})

	return t.chooseSoManyStorages(red.RepCount, lo.Map(sortedStgs, func(storage *rechooseStorage, idx int) *StorageLoadInfo { return storage.StorageLoadInfo }))
}

func (t *CheckPackageRedundancy) rechooseStoragesForEC(obj stgmod.ObjectDetail, red *cdssdk.ECRedundancy, allStgs map[cdssdk.StorageID]*StorageLoadInfo) []*StorageLoadInfo {
	type rechooseStg struct {
		*StorageLoadInfo
		CachedBlockIndex int
	}

	var rechooseStgs []*rechooseStg
	for _, stg := range allStgs {
		cachedBlockIndex := -1
		for _, block := range obj.Blocks {
			if block.StorageID == stg.Storage.Storage.StorageID {
				cachedBlockIndex = block.Index
				break
			}
		}

		rechooseStgs = append(rechooseStgs, &rechooseStg{
			StorageLoadInfo:  stg,
			CachedBlockIndex: cachedBlockIndex,
		})
	}

	sortedStgs := sort2.Sort(rechooseStgs, func(left *rechooseStg, right *rechooseStg) int {
		// 已经缓存了文件块的节点优先选择
		v := sort2.CmpBool(right.CachedBlockIndex > -1, left.CachedBlockIndex > -1)
		if v != 0 {
			return v
		}

		return sort2.Cmp(right.AccessAmount, left.AccessAmount)
	})

	// TODO 可以考虑选择已有块的节点时，能依然按照Index顺序选择
	return t.chooseSoManyStorages(red.N, lo.Map(sortedStgs, func(storage *rechooseStg, idx int) *StorageLoadInfo { return storage.StorageLoadInfo }))
}

func (t *CheckPackageRedundancy) rechooseStoragesForLRC(obj stgmod.ObjectDetail, red *cdssdk.LRCRedundancy, allStgs map[cdssdk.StorageID]*StorageLoadInfo) []*StorageLoadInfo {
	type rechooseStg struct {
		*StorageLoadInfo
		CachedBlockIndex int
	}

	var rechooseStgs []*rechooseStg
	for _, stg := range allStgs {
		cachedBlockIndex := -1
		for _, block := range obj.Blocks {
			if block.StorageID == stg.Storage.Storage.StorageID {
				cachedBlockIndex = block.Index
				break
			}
		}

		rechooseStgs = append(rechooseStgs, &rechooseStg{
			StorageLoadInfo:  stg,
			CachedBlockIndex: cachedBlockIndex,
		})
	}

	sortedStgs := sort2.Sort(rechooseStgs, func(left *rechooseStg, right *rechooseStg) int {
		// 已经缓存了文件块的节点优先选择
		v := sort2.CmpBool(right.CachedBlockIndex > -1, left.CachedBlockIndex > -1)
		if v != 0 {
			return v
		}

		return sort2.Cmp(right.AccessAmount, left.AccessAmount)
	})

	// TODO 可以考虑选择已有块的节点时，能依然按照Index顺序选择
	return t.chooseSoManyStorages(red.N, lo.Map(sortedStgs, func(storage *rechooseStg, idx int) *StorageLoadInfo { return storage.StorageLoadInfo }))
}

func (t *CheckPackageRedundancy) chooseSoManyStorages(count int, stgs []*StorageLoadInfo) []*StorageLoadInfo {
	repeateCount := (count + len(stgs) - 1) / len(stgs)
	extendStgs := make([]*StorageLoadInfo, repeateCount*len(stgs))

	// 使用复制的方式将节点数扩充到要求的数量
	// 复制之后的结构：ABCD -> AAABBBCCCDDD
	for p := 0; p < repeateCount; p++ {
		for i, storage := range stgs {
			putIdx := i*repeateCount + p
			extendStgs[putIdx] = storage
		}
	}
	extendStgs = extendStgs[:count]

	var chosen []*StorageLoadInfo
	for len(chosen) < count {
		// 在每一轮内都选不同地区的节点，如果节点数不够，那么就再来一轮
		chosenLocations := make(map[cdssdk.LocationID]bool)
		for i, stg := range extendStgs {
			if stg == nil {
				continue
			}

			if chosenLocations[stg.Storage.MasterHub.LocationID] {
				continue
			}

			chosen = append(chosen, stg)
			chosenLocations[stg.Storage.MasterHub.LocationID] = true
			extendStgs[i] = nil
		}
	}

	return chosen
}

func (t *CheckPackageRedundancy) noneToRep(ctx ExecuteContext, obj stgmod.ObjectDetail, red *cdssdk.RepRedundancy, uploadStgs []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	if len(obj.Blocks) == 0 {
		return nil, fmt.Errorf("object is not cached on any storages, cannot change its redundancy to rep")
	}

	srcStg, ok := allStgs[obj.Blocks[0].StorageID]
	if !ok {
		return nil, fmt.Errorf("storage %v not found", obj.Blocks[0].StorageID)
	}
	if srcStg.Storage.MasterHub == nil {
		return nil, fmt.Errorf("storage %v has no master hub", obj.Blocks[0].StorageID)
	}

	// 如果选择的备份节点都是同一个，那么就只要上传一次
	uploadStgs = lo.UniqBy(uploadStgs, func(item *StorageLoadInfo) cdssdk.StorageID { return item.Storage.Storage.StorageID })

	ft := ioswitch2.NewFromTo()
	ft.AddFrom(ioswitch2.NewFromShardstore(obj.Object.FileHash, *srcStg.Storage.MasterHub, srcStg.Storage.Storage, ioswitch2.RawStream()))
	for i, stg := range uploadStgs {
		ft.AddTo(ioswitch2.NewToShardStore(*stg.Storage.MasterHub, stg.Storage.Storage, ioswitch2.RawStream(), fmt.Sprintf("%d", i)))
	}

	plans := exec.NewPlanBuilder()
	err := parser.Parse(ft, plans)
	if err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	// TODO 添加依赖
	execCtx := exec.NewExecContext()
	exec.SetValueByType(execCtx, ctx.Args.StgMgr)
	ret, err := plans.Execute(execCtx).Wait(context.Background())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i, stg := range uploadStgs {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID:  obj.Object.ObjectID,
			Index:     0,
			StorageID: stg.Storage.Storage.StorageID,
			FileHash:  ret[fmt.Sprintf("%d", i)].(*ops2.FileHashValue).Hash,
		})
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) noneToEC(ctx ExecuteContext, obj stgmod.ObjectDetail, red *cdssdk.ECRedundancy, uploadStgs []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	if len(obj.Blocks) == 0 {
		return nil, fmt.Errorf("object is not cached on any storages, cannot change its redundancy to ec")
	}

	srcStg, ok := allStgs[obj.Blocks[0].StorageID]
	if !ok {
		return nil, fmt.Errorf("storage %v not found", obj.Blocks[0].StorageID)
	}
	if srcStg.Storage.MasterHub == nil {
		return nil, fmt.Errorf("storage %v has no master hub", obj.Blocks[0].StorageID)
	}

	ft := ioswitch2.NewFromTo()
	ft.ECParam = red
	ft.AddFrom(ioswitch2.NewFromShardstore(obj.Object.FileHash, *srcStg.Storage.MasterHub, srcStg.Storage.Storage, ioswitch2.RawStream()))
	for i := 0; i < red.N; i++ {
		ft.AddTo(ioswitch2.NewToShardStore(*uploadStgs[i].Storage.MasterHub, uploadStgs[i].Storage.Storage, ioswitch2.ECSrteam(i), fmt.Sprintf("%d", i)))
	}
	plans := exec.NewPlanBuilder()
	err := parser.Parse(ft, plans)
	if err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	execCtx := exec.NewExecContext()
	exec.SetValueByType(execCtx, ctx.Args.StgMgr)
	ioRet, err := plans.Execute(execCtx).Wait(context.Background())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i := 0; i < red.N; i++ {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID:  obj.Object.ObjectID,
			Index:     i,
			StorageID: uploadStgs[i].Storage.Storage.StorageID,
			FileHash:  ioRet[fmt.Sprintf("%d", i)].(*ops2.FileHashValue).Hash,
		})
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) noneToLRC(ctx ExecuteContext, obj stgmod.ObjectDetail, red *cdssdk.LRCRedundancy, uploadStorages []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	if len(obj.Blocks) == 0 {
		return nil, fmt.Errorf("object is not cached on any storages, cannot change its redundancy to ec")
	}

	srcStg, ok := allStgs[obj.Blocks[0].StorageID]
	if !ok {
		return nil, fmt.Errorf("storage %v not found", obj.Blocks[0].StorageID)
	}
	if srcStg.Storage.MasterHub == nil {
		return nil, fmt.Errorf("storage %v has no master hub", obj.Blocks[0].StorageID)
	}

	var toes []ioswitchlrc.To
	for i := 0; i < red.N; i++ {
		toes = append(toes, ioswitchlrc.NewToStorage(*uploadStorages[i].Storage.MasterHub, uploadStorages[i].Storage.Storage, i, fmt.Sprintf("%d", i)))
	}

	plans := exec.NewPlanBuilder()
	err := lrcparser.Encode(ioswitchlrc.NewFromStorage(obj.Object.FileHash, *srcStg.Storage.MasterHub, srcStg.Storage.Storage, -1), toes, plans)
	if err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	execCtx := exec.NewExecContext()
	exec.SetValueByType(execCtx, ctx.Args.StgMgr)
	ioRet, err := plans.Execute(execCtx).Wait(context.Background())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i := 0; i < red.N; i++ {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID:  obj.Object.ObjectID,
			Index:     i,
			StorageID: uploadStorages[i].Storage.Storage.StorageID,
			FileHash:  ioRet[fmt.Sprintf("%d", i)].(*ops2.FileHashValue).Hash,
		})
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) noneToSeg(ctx ExecuteContext, obj stgmod.ObjectDetail, red *cdssdk.SegmentRedundancy, uploadStgs []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	if len(obj.Blocks) == 0 {
		return nil, fmt.Errorf("object is not cached on any storages, cannot change its redundancy to rep")
	}

	srcStg, ok := allStgs[obj.Blocks[0].StorageID]
	if !ok {
		return nil, fmt.Errorf("storage %v not found", obj.Blocks[0].StorageID)
	}
	if srcStg.Storage.MasterHub == nil {
		return nil, fmt.Errorf("storage %v has no master hub", obj.Blocks[0].StorageID)
	}

	// 如果选择的备份节点都是同一个，那么就只要上传一次
	uploadStgs = lo.UniqBy(uploadStgs, func(item *StorageLoadInfo) cdssdk.StorageID { return item.Storage.Storage.StorageID })

	ft := ioswitch2.NewFromTo()
	ft.SegmentParam = red
	ft.AddFrom(ioswitch2.NewFromShardstore(obj.Object.FileHash, *srcStg.Storage.MasterHub, srcStg.Storage.Storage, ioswitch2.RawStream()))
	for i, stg := range uploadStgs {
		ft.AddTo(ioswitch2.NewToShardStore(*stg.Storage.MasterHub, stg.Storage.Storage, ioswitch2.SegmentStream(i), fmt.Sprintf("%d", i)))
	}

	plans := exec.NewPlanBuilder()
	err := parser.Parse(ft, plans)
	if err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	// TODO 添加依赖
	execCtx := exec.NewExecContext()
	exec.SetValueByType(execCtx, ctx.Args.StgMgr)
	ret, err := plans.Execute(execCtx).Wait(context.Background())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i, stg := range uploadStgs {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID:  obj.Object.ObjectID,
			Index:     i,
			StorageID: stg.Storage.Storage.StorageID,
			FileHash:  ret[fmt.Sprintf("%d", i)].(*ops2.FileHashValue).Hash,
		})
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) repToRep(ctx ExecuteContext, obj stgmod.ObjectDetail, red *cdssdk.RepRedundancy, uploadStgs []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	if len(obj.Blocks) == 0 {
		return nil, fmt.Errorf("object is not cached on any storages, cannot change its redundancy to rep")
	}

	srcStg, ok := allStgs[obj.Blocks[0].StorageID]
	if !ok {
		return nil, fmt.Errorf("storage %v not found", obj.Blocks[0].StorageID)
	}
	if srcStg.Storage.MasterHub == nil {
		return nil, fmt.Errorf("storage %v has no master hub", obj.Blocks[0].StorageID)
	}

	// 如果选择的备份节点都是同一个，那么就只要上传一次
	uploadStgs = lo.UniqBy(uploadStgs, func(item *StorageLoadInfo) cdssdk.StorageID { return item.Storage.Storage.StorageID })

	ft := ioswitch2.NewFromTo()
	ft.AddFrom(ioswitch2.NewFromShardstore(obj.Object.FileHash, *srcStg.Storage.MasterHub, srcStg.Storage.Storage, ioswitch2.RawStream()))
	for i, stg := range uploadStgs {
		ft.AddTo(ioswitch2.NewToShardStore(*stg.Storage.MasterHub, stg.Storage.Storage, ioswitch2.RawStream(), fmt.Sprintf("%d", i)))
	}

	plans := exec.NewPlanBuilder()
	err := parser.Parse(ft, plans)
	if err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	// TODO 添加依赖
	execCtx := exec.NewExecContext()
	exec.SetValueByType(execCtx, ctx.Args.StgMgr)
	ret, err := plans.Execute(execCtx).Wait(context.Background())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i, stg := range uploadStgs {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID:  obj.Object.ObjectID,
			Index:     0,
			StorageID: stg.Storage.Storage.StorageID,
			FileHash:  ret[fmt.Sprintf("%d", i)].(*ops2.FileHashValue).Hash,
		})
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) repToEC(ctx ExecuteContext, obj stgmod.ObjectDetail, red *cdssdk.ECRedundancy, uploadStorages []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	return t.noneToEC(ctx, obj, red, uploadStorages, allStgs)
}

func (t *CheckPackageRedundancy) ecToRep(ctx ExecuteContext, obj stgmod.ObjectDetail, srcRed *cdssdk.ECRedundancy, tarRed *cdssdk.RepRedundancy, uploadStgs []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	var chosenBlocks []stgmod.GrouppedObjectBlock
	var chosenBlockIndexes []int
	var chosenBlockStg []stgmod.StorageDetail
	for _, block := range obj.GroupBlocks() {
		if len(block.StorageIDs) > 0 {
			// TODO 考虑选择最优的节点
			stg, ok := allStgs[block.StorageIDs[0]]
			if !ok {
				continue
			}
			if stg.Storage.MasterHub == nil {
				continue
			}

			chosenBlocks = append(chosenBlocks, block)
			chosenBlockIndexes = append(chosenBlockIndexes, block.Index)
			chosenBlockStg = append(chosenBlockStg, stg.Storage)
		}

		if len(chosenBlocks) == srcRed.K {
			break
		}
	}

	if len(chosenBlocks) < srcRed.K {
		return nil, fmt.Errorf("no enough blocks to reconstruct the original file data")
	}

	// 如果选择的备份节点都是同一个，那么就只要上传一次
	uploadStgs = lo.UniqBy(uploadStgs, func(item *StorageLoadInfo) cdssdk.StorageID { return item.Storage.Storage.StorageID })

	// 每个被选节点都在自己节点上重建原始数据
	planBlder := exec.NewPlanBuilder()
	for i := range uploadStgs {
		ft := ioswitch2.NewFromTo()
		ft.ECParam = srcRed

		for i2, block := range chosenBlocks {
			ft.AddFrom(ioswitch2.NewFromShardstore(block.FileHash, *chosenBlockStg[i2].MasterHub, chosenBlockStg[i2].Storage, ioswitch2.ECSrteam(block.Index)))
		}

		len := obj.Object.Size
		ft.AddTo(ioswitch2.NewToShardStoreWithRange(*uploadStgs[i].Storage.MasterHub, uploadStgs[i].Storage.Storage, ioswitch2.RawStream(), fmt.Sprintf("%d", i), exec.Range{
			Offset: 0,
			Length: &len,
		}))

		err := parser.Parse(ft, planBlder)
		if err != nil {
			return nil, fmt.Errorf("parsing plan: %w", err)
		}
	}

	// TODO 添加依赖
	execCtx := exec.NewExecContext()
	exec.SetValueByType(execCtx, ctx.Args.StgMgr)
	ioRet, err := planBlder.Execute(execCtx).Wait(context.Background())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i := range uploadStgs {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID:  obj.Object.ObjectID,
			Index:     0,
			StorageID: uploadStgs[i].Storage.Storage.StorageID,
			FileHash:  ioRet[fmt.Sprintf("%d", i)].(*ops2.FileHashValue).Hash,
		})
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: tarRed,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) ecToEC(ctx ExecuteContext, obj stgmod.ObjectDetail, srcRed *cdssdk.ECRedundancy, tarRed *cdssdk.ECRedundancy, uploadStorages []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	grpBlocks := obj.GroupBlocks()

	var chosenBlocks []stgmod.GrouppedObjectBlock
	var chosenBlockStg []stgmod.StorageDetail
	for _, block := range grpBlocks {
		if len(block.StorageIDs) > 0 {
			stg, ok := allStgs[block.StorageIDs[0]]
			if !ok {
				continue
			}
			if stg.Storage.MasterHub == nil {
				continue
			}

			chosenBlocks = append(chosenBlocks, block)
			chosenBlockStg = append(chosenBlockStg, stg.Storage)
		}

		if len(chosenBlocks) == srcRed.K {
			break
		}
	}

	if len(chosenBlocks) < srcRed.K {
		return nil, fmt.Errorf("no enough blocks to reconstruct the original file data")
	}

	// 目前EC的参数都相同，所以可以不用重建出完整数据然后再分块，可以直接构建出目的节点需要的块
	planBlder := exec.NewPlanBuilder()

	var newBlocks []stgmod.ObjectBlock
	shouldUpdateBlocks := false
	for i, stg := range uploadStorages {
		newBlock := stgmod.ObjectBlock{
			ObjectID:  obj.Object.ObjectID,
			Index:     i,
			StorageID: stg.Storage.Storage.StorageID,
		}

		grp, ok := lo.Find(grpBlocks, func(grp stgmod.GrouppedObjectBlock) bool { return grp.Index == i })

		// 如果新选中的节点已经记录在Block表中，那么就不需要任何变更
		if ok && lo.Contains(grp.StorageIDs, stg.Storage.Storage.StorageID) {
			newBlock.FileHash = grp.FileHash
			newBlocks = append(newBlocks, newBlock)
			continue
		}

		shouldUpdateBlocks = true

		// 否则就要重建出这个节点需要的块

		ft := ioswitch2.NewFromTo()
		ft.ECParam = srcRed
		for i2, block := range chosenBlocks {
			ft.AddFrom(ioswitch2.NewFromShardstore(block.FileHash, *chosenBlockStg[i2].MasterHub, chosenBlockStg[i2].Storage, ioswitch2.ECSrteam(block.Index)))
		}

		// 输出只需要自己要保存的那一块
		ft.AddTo(ioswitch2.NewToShardStore(*stg.Storage.MasterHub, stg.Storage.Storage, ioswitch2.ECSrteam(i), fmt.Sprintf("%d", i)))

		err := parser.Parse(ft, planBlder)
		if err != nil {
			return nil, fmt.Errorf("parsing plan: %w", err)
		}

		newBlocks = append(newBlocks, newBlock)
	}

	// 如果没有任何Plan，Wait会直接返回成功
	execCtx := exec.NewExecContext()
	exec.SetValueByType(execCtx, ctx.Args.StgMgr)
	ret, err := planBlder.Execute(execCtx).Wait(context.Background())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	if !shouldUpdateBlocks {
		return nil, nil
	}

	for k, v := range ret {
		idx, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing result key %s as index: %w", k, err)
		}

		newBlocks[idx].FileHash = v.(*ops2.FileHashValue).Hash
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: tarRed,
		Blocks:     newBlocks,
	}, nil
}

func (t *CheckPackageRedundancy) lrcToLRC(ctx ExecuteContext, obj stgmod.ObjectDetail, srcRed *cdssdk.LRCRedundancy, tarRed *cdssdk.LRCRedundancy, uploadStorages []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {

	blocksGrpByIndex := obj.GroupBlocks()

	var lostBlocks []int
	var lostBlockGrps []int
	canGroupReconstruct := true

	allBlockFlags := make([]bool, srcRed.N)
	for _, block := range blocksGrpByIndex {
		allBlockFlags[block.Index] = true
	}

	for i, ok := range allBlockFlags {
		grpID := srcRed.FindGroup(i)
		if !ok {
			if grpID == -1 {
				canGroupReconstruct = false
				break
			}

			if len(lostBlocks) > 0 && lostBlockGrps[len(lostBlockGrps)-1] == grpID {
				canGroupReconstruct = false
				break
			}

			lostBlocks = append(lostBlocks, i)
			lostBlockGrps = append(lostBlockGrps, grpID)
		}
	}

	if canGroupReconstruct {
		// 	return t.groupReconstructLRC(obj, lostBlocks, lostBlockGrps, blocksGrpByIndex, srcRed, uploadStorages)
	}

	return t.reconstructLRC(ctx, obj, blocksGrpByIndex, srcRed, uploadStorages, allStgs)
}

/*
TODO2 修复这一块的代码

	func (t *CheckPackageRedundancy) groupReconstructLRC(obj stgmod.ObjectDetail, lostBlocks []int, lostBlockGrps []int, grpedBlocks []stgmod.GrouppedObjectBlock, red *cdssdk.LRCRedundancy, uploadStorages []*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
		grped := make(map[int]stgmod.GrouppedObjectBlock)
		for _, b := range grpedBlocks {
			grped[b.Index] = b
		}

		plans := exec.NewPlanBuilder()

		for i := 0; i < len(lostBlocks); i++ {
			var froms []ioswitchlrc.From
			grpEles := red.GetGroupElements(lostBlockGrps[i])
			for _, ele := range grpEles {
				if ele == lostBlocks[i] {
					continue
				}

				froms = append(froms, ioswitchlrc.NewFromStorage(grped[ele].FileHash, nil, ele))
			}

			err := lrcparser.ReconstructGroup(froms, []ioswitchlrc.To{
				ioswitchlrc.NewToStorage(uploadStorages[i].Storage, lostBlocks[i], fmt.Sprintf("%d", lostBlocks[i])),
			}, plans)
			if err != nil {
				return nil, fmt.Errorf("parsing plan: %w", err)
			}
		}

		fmt.Printf("plans: %v\n", plans)

		// 如果没有任何Plan，Wait会直接返回成功
		// TODO 添加依赖
		ret, err := plans.Execute(exec.NewExecContext()).Wait(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("executing io plan: %w", err)
		}

		var newBlocks []stgmod.ObjectBlock
		for _, i := range lostBlocks {
			newBlocks = append(newBlocks, stgmod.ObjectBlock{
				ObjectID:  obj.Object.ObjectID,
				Index:     i,
				StorageID: uploadStorages[i].Storage.Storage.StorageID,
				FileHash:  ret[fmt.Sprintf("%d", i)].(*ops2.FileHashValue).Hash,
			})
		}
		for _, b := range grpedBlocks {
			for _, hubID := range b.StorageIDs {
				newBlocks = append(newBlocks, stgmod.ObjectBlock{
					ObjectID:  obj.Object.ObjectID,
					Index:     b.Index,
					StorageID: hubID,
					FileHash:  b.FileHash,
				})
			}
		}

		return &coormq.UpdatingObjectRedundancy{
			ObjectID:   obj.Object.ObjectID,
			Redundancy: red,
			Blocks:     newBlocks,
		}, nil
	}
*/
func (t *CheckPackageRedundancy) reconstructLRC(ctx ExecuteContext, obj stgmod.ObjectDetail, grpBlocks []stgmod.GrouppedObjectBlock, red *cdssdk.LRCRedundancy, uploadStorages []*StorageLoadInfo, allStgs map[cdssdk.StorageID]*StorageLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	var chosenBlocks []stgmod.GrouppedObjectBlock
	var chosenBlockStg []stgmod.StorageDetail

	for _, block := range grpBlocks {
		if len(block.StorageIDs) > 0 && block.Index < red.M() {
			stg, ok := allStgs[block.StorageIDs[0]]
			if !ok {
				continue
			}
			if stg.Storage.MasterHub == nil {
				continue
			}

			chosenBlocks = append(chosenBlocks, block)
			chosenBlockStg = append(chosenBlockStg, stg.Storage)
		}

		if len(chosenBlocks) == red.K {
			break
		}
	}

	if len(chosenBlocks) < red.K {
		return nil, fmt.Errorf("no enough blocks to reconstruct the original file data")
	}

	// 目前LRC的参数都相同，所以可以不用重建出完整数据然后再分块，可以直接构建出目的节点需要的块
	planBlder := exec.NewPlanBuilder()

	var froms []ioswitchlrc.From
	var toes []ioswitchlrc.To
	var newBlocks []stgmod.ObjectBlock
	shouldUpdateBlocks := false
	for i, storage := range uploadStorages {
		newBlock := stgmod.ObjectBlock{
			ObjectID:  obj.Object.ObjectID,
			Index:     i,
			StorageID: storage.Storage.Storage.StorageID,
		}

		grp, ok := lo.Find(grpBlocks, func(grp stgmod.GrouppedObjectBlock) bool { return grp.Index == i })

		// 如果新选中的节点已经记录在Block表中，那么就不需要任何变更
		if ok && lo.Contains(grp.StorageIDs, storage.Storage.Storage.StorageID) {
			newBlock.FileHash = grp.FileHash
			newBlocks = append(newBlocks, newBlock)
			continue
		}

		shouldUpdateBlocks = true

		// 否则就要重建出这个节点需要的块

		for i2, block := range chosenBlocks {
			froms = append(froms, ioswitchlrc.NewFromStorage(block.FileHash, *chosenBlockStg[i2].MasterHub, chosenBlockStg[i2].Storage, block.Index))
		}

		// 输出只需要自己要保存的那一块
		toes = append(toes, ioswitchlrc.NewToStorage(*storage.Storage.MasterHub, storage.Storage.Storage, i, fmt.Sprintf("%d", i)))

		newBlocks = append(newBlocks, newBlock)
	}

	err := lrcparser.ReconstructAny(froms, toes, planBlder)
	if err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	fmt.Printf("plans: %v\n", planBlder)

	// 如果没有任何Plan，Wait会直接返回成功
	execCtx := exec.NewExecContext()
	exec.SetValueByType(execCtx, ctx.Args.StgMgr)
	ret, err := planBlder.Execute(execCtx).Wait(context.Background())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	if !shouldUpdateBlocks {
		return nil, nil
	}

	for k, v := range ret {
		idx, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing result key %s as index: %w", k, err)
		}

		newBlocks[idx].FileHash = v.(*ops2.FileHashValue).Hash
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     newBlocks,
	}, nil
}

// func (t *CheckPackageRedundancy) pinObject(hubID cdssdk.HubID, fileHash string) error {
// 	agtCli, err := stgglb.AgentMQPool.Acquire(hubID)
// 	if err != nil {
// 		return fmt.Errorf("new agent client: %w", err)
// 	}
// 	defer stgglb.AgentMQPool.Release(agtCli)

// 	_, err = agtCli.PinObject(agtmq.ReqPinObject([]string{fileHash}, false))
// 	if err != nil {
// 		return fmt.Errorf("start pinning object: %w", err)
// 	}

// 	return nil
// }

func init() {
	RegisterMessageConvertor(NewCheckPackageRedundancy)
}
