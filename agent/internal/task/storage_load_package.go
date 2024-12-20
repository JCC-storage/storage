package task

import (
	"fmt"
	"io"
	"math"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

type StorageLoadPackage struct {
	PackagePath string
	LocalBase   string
	RemoteBase  string

	userID       cdssdk.UserID
	packageID    cdssdk.PackageID
	storageID    cdssdk.StorageID
	pinnedBlocks []stgmod.ObjectBlock
}

func NewStorageLoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID) *StorageLoadPackage {
	return &StorageLoadPackage{
		userID:    userID,
		packageID: packageID,
		storageID: storageID,
	}
}
func (t *StorageLoadPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	startTime := time.Now()
	log := logger.WithType[StorageLoadPackage]("Task")
	log.WithField("TaskID", task.ID()).
		Infof("begin to load package %v to %v", t.packageID, t.storageID)

	err := t.do(task, ctx)
	if err == nil {
		log.WithField("TaskID", task.ID()).
			Infof("loading success, cost: %v", time.Since(startTime))
	} else {
		log.WithField("TaskID", task.ID()).
			Warnf("loading package: %v, cost: %v", err, time.Since(startTime))
	}

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *StorageLoadPackage) do(task *task.Task[TaskContext], ctx TaskContext) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	shared, err := ctx.stgMgr.GetSharedStore(t.storageID)
	if err != nil {
		return fmt.Errorf("get shared store of storage %v: %w", t.storageID, err)
	}
	t.PackagePath = utils.MakeLoadedPackagePath(t.userID, t.packageID)

	getObjectDetails, err := coorCli.GetPackageObjectDetails(coormq.ReqGetPackageObjectDetails(t.packageID))
	if err != nil {
		return fmt.Errorf("getting package object details: %w", err)
	}

	shardstore, err := ctx.stgMgr.GetShardStore(t.storageID)
	if err != nil {
		return fmt.Errorf("get shard store of storage %v: %w", t.storageID, err)
	}

	mutex, err := reqbuilder.NewBuilder().
		// 提前占位
		Metadata().StoragePackage().CreateOne(t.userID, t.storageID, t.packageID).
		// 保护在storage目录中下载的文件
		Storage().Buzy(t.storageID).
		// 保护下载文件时同时保存到IPFS的文件
		Shard().Buzy(t.storageID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	for _, obj := range getObjectDetails.Objects {
		err := t.downloadOne(coorCli, shardstore, shared, obj)
		if err != nil {
			return err
		}
		ctx.accessStat.AddAccessCounter(obj.Object.ObjectID, t.packageID, t.storageID, 1)
	}

	_, err = coorCli.StoragePackageLoaded(coormq.NewStoragePackageLoaded(t.userID, t.storageID, t.packageID, t.pinnedBlocks))
	if err != nil {
		return fmt.Errorf("loading package to storage: %w", err)
	}

	// TODO 要防止下载的临时文件被删除
	return err
}

func (t *StorageLoadPackage) downloadOne(coorCli *coormq.Client, shardStore types.ShardStore, shared types.SharedStore, obj stgmod.ObjectDetail) error {
	var file io.ReadCloser

	switch red := obj.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		reader, err := t.downloadNoneOrRepObject(shardStore, obj)
		if err != nil {
			return fmt.Errorf("downloading object: %w", err)
		}
		file = reader

	case *cdssdk.RepRedundancy:
		reader, err := t.downloadNoneOrRepObject(shardStore, obj)
		if err != nil {
			return fmt.Errorf("downloading rep object: %w", err)
		}
		file = reader

	case *cdssdk.ECRedundancy:
		reader, pinnedBlocks, err := t.downloadECObject(coorCli, shardStore, obj, red)
		if err != nil {
			return fmt.Errorf("downloading ec object: %w", err)
		}
		file = reader
		t.pinnedBlocks = append(t.pinnedBlocks, pinnedBlocks...)

	default:
		return fmt.Errorf("unknow redundancy type: %v", reflect2.TypeOfValue(obj.Object.Redundancy))
	}
	defer file.Close()

	if _, err := shared.WritePackageObject(t.userID, t.packageID, obj.Object.Path, file); err != nil {
		return fmt.Errorf("writting object to file: %w", err)
	}

	return nil
}

func (t *StorageLoadPackage) downloadNoneOrRepObject(shardStore types.ShardStore, obj stgmod.ObjectDetail) (io.ReadCloser, error) {
	if len(obj.Blocks) == 0 && len(obj.PinnedAt) == 0 {
		return nil, fmt.Errorf("no storage has this object")
	}

	file, err := shardStore.Open(types.NewOpen(obj.Object.FileHash))
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (t *StorageLoadPackage) downloadECObject(coorCli *coormq.Client, shardStore types.ShardStore, obj stgmod.ObjectDetail, ecRed *cdssdk.ECRedundancy) (io.ReadCloser, []stgmod.ObjectBlock, error) {
	allStorages, err := t.sortDownloadStorages(coorCli, obj)
	if err != nil {
		return nil, nil, err
	}
	bsc, blocks := t.getMinReadingBlockSolution(allStorages, ecRed.K)
	osc, _ := t.getMinReadingObjectSolution(allStorages, ecRed.K)
	if bsc < osc {
		var fileStrs []io.ReadCloser

		rs, err := ec.NewStreamRs(ecRed.K, ecRed.N, ecRed.ChunkSize)
		if err != nil {
			return nil, nil, fmt.Errorf("new rs: %w", err)
		}

		for i := range blocks {
			str, err := shardStore.Open(types.NewOpen(blocks[i].Block.FileHash))
			if err != nil {
				for i -= 1; i >= 0; i-- {
					fileStrs[i].Close()
				}
				return nil, nil, fmt.Errorf("donwloading file: %w", err)
			}

			fileStrs = append(fileStrs, str)
		}

		fileReaders, filesCloser := io2.ToReaders(fileStrs)

		var indexes []int
		for _, b := range blocks {
			indexes = append(indexes, b.Block.Index)
		}

		outputs, outputsCloser := io2.ToReaders(rs.ReconstructData(fileReaders, indexes))
		return io2.AfterReadClosed(io2.Length(io2.ChunkedJoin(outputs, int(ecRed.ChunkSize)), obj.Object.Size), func(c io.ReadCloser) {
			filesCloser()
			outputsCloser()
		}), nil, nil
	}

	// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
	if osc == math.MaxFloat64 {
		return nil, nil, fmt.Errorf("no enough blocks to reconstruct the file, want %d, get only %d", ecRed.K, len(blocks))
	}

	// 如果是直接读取的文件，那么就不需要Pin文件块
	str, err := shardStore.Open(types.NewOpen(obj.Object.FileHash))
	return str, nil, err
}

type downloadStorageInfo struct {
	Storage      stgmod.StorageDetail
	ObjectPinned bool
	Blocks       []stgmod.ObjectBlock
	Distance     float64
}

func (t *StorageLoadPackage) sortDownloadStorages(coorCli *coormq.Client, obj stgmod.ObjectDetail) ([]*downloadStorageInfo, error) {
	var stgIDs []cdssdk.StorageID
	for _, id := range obj.PinnedAt {
		if !lo.Contains(stgIDs, id) {
			stgIDs = append(stgIDs, id)
		}
	}
	for _, b := range obj.Blocks {
		if !lo.Contains(stgIDs, b.StorageID) {
			stgIDs = append(stgIDs, b.StorageID)
		}
	}

	getStgs, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails(stgIDs))
	if err != nil {
		return nil, fmt.Errorf("getting storage details: %w", err)
	}
	allStgs := make(map[cdssdk.StorageID]stgmod.StorageDetail)
	for _, stg := range getStgs.Storages {
		allStgs[stg.Storage.StorageID] = *stg
	}

	downloadStorageMap := make(map[cdssdk.StorageID]*downloadStorageInfo)
	for _, id := range obj.PinnedAt {
		storage, ok := downloadStorageMap[id]
		if !ok {
			mod := allStgs[id]
			storage = &downloadStorageInfo{
				Storage:      mod,
				ObjectPinned: true,
				Distance:     t.getStorageDistance(mod),
			}
			downloadStorageMap[id] = storage
		}

		storage.ObjectPinned = true
	}

	for _, b := range obj.Blocks {
		storage, ok := downloadStorageMap[b.StorageID]
		if !ok {
			mod := allStgs[b.StorageID]
			storage = &downloadStorageInfo{
				Storage:  mod,
				Distance: t.getStorageDistance(mod),
			}
			downloadStorageMap[b.StorageID] = storage
		}

		storage.Blocks = append(storage.Blocks, b)
	}

	return sort2.Sort(lo.Values(downloadStorageMap), func(left, right *downloadStorageInfo) int {
		return sort2.Cmp(left.Distance, right.Distance)
	}), nil
}

type downloadBlock struct {
	Storage stgmod.StorageDetail
	Block   stgmod.ObjectBlock
}

func (t *StorageLoadPackage) getMinReadingBlockSolution(sortedStorages []*downloadStorageInfo, k int) (float64, []downloadBlock) {
	gotBlocksMap := bitmap.Bitmap64(0)
	var gotBlocks []downloadBlock
	dist := float64(0.0)
	for _, n := range sortedStorages {
		for _, b := range n.Blocks {
			if !gotBlocksMap.Get(b.Index) {
				gotBlocks = append(gotBlocks, downloadBlock{
					Storage: n.Storage,
					Block:   b,
				})
				gotBlocksMap.Set(b.Index, true)
				dist += n.Distance
			}

			if len(gotBlocks) >= k {
				return dist, gotBlocks
			}
		}
	}

	return math.MaxFloat64, gotBlocks
}

func (t *StorageLoadPackage) getMinReadingObjectSolution(sortedStorages []*downloadStorageInfo, k int) (float64, *stgmod.StorageDetail) {
	dist := math.MaxFloat64
	var downloadStg *stgmod.StorageDetail
	for _, n := range sortedStorages {
		if n.ObjectPinned && float64(k)*n.Distance < dist {
			dist = float64(k) * n.Distance
			stg := n.Storage
			downloadStg = &stg
		}
	}

	return dist, downloadStg
}

func (t *StorageLoadPackage) getStorageDistance(stg stgmod.StorageDetail) float64 {
	if stgglb.Local.HubID != nil {
		if stg.MasterHub.HubID == *stgglb.Local.HubID {
			return consts.StorageDistanceSameStorage
		}
	}

	if stg.MasterHub.LocationID == stgglb.Local.LocationID {
		return consts.StorageDistanceSameLocation
	}

	return consts.StorageDistanceOther
}
