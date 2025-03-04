package uploader

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	"gitlink.org.cn/cloudream/storage/common/pkgs/metacache"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
)

type Uploader struct {
	distlock     *distlock.Service
	connectivity *connectivity.Collector
	stgAgts      *agtpool.AgentPool
	stgMeta      *metacache.StorageMeta
}

func NewUploader(distlock *distlock.Service, connectivity *connectivity.Collector, stgAgts *agtpool.AgentPool, stgMeta *metacache.StorageMeta) *Uploader {
	return &Uploader{
		distlock:     distlock,
		connectivity: connectivity,
		stgAgts:      stgAgts,
		stgMeta:      stgMeta,
	}
}

func (u *Uploader) BeginUpdate(userID cdssdk.UserID, pkgID cdssdk.PackageID, affinity cdssdk.StorageID, loadTo []cdssdk.StorageID, loadToPath []string) (*UpdateUploader, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getUserStgsResp, err := coorCli.GetUserStorageDetails(coormq.ReqGetUserStorageDetails(userID))
	if err != nil {
		return nil, fmt.Errorf("getting user storages: %w", err)
	}

	cons := u.connectivity.GetAll()
	var userStgs []UploadStorageInfo
	for _, stg := range getUserStgsResp.Storages {
		if stg.MasterHub == nil {
			continue
		}

		delay := time.Duration(math.MaxInt64)

		con, ok := cons[stg.MasterHub.HubID]
		if ok && con.Latency != nil {
			delay = *con.Latency
		}

		userStgs = append(userStgs, UploadStorageInfo{
			Storage:        stg,
			Delay:          delay,
			IsSameLocation: stg.MasterHub.LocationID == stgglb.Local.LocationID,
		})
	}

	if len(userStgs) == 0 {
		return nil, fmt.Errorf("user no available storages")
	}

	loadToStgs := make([]stgmod.StorageDetail, len(loadTo))
	for i, stgID := range loadTo {
		stg, ok := lo.Find(getUserStgsResp.Storages, func(stg stgmod.StorageDetail) bool {
			return stg.Storage.StorageID == stgID
		})
		if !ok {
			return nil, fmt.Errorf("load to storage %v not found", stgID)
		}
		if stg.MasterHub == nil {
			return nil, fmt.Errorf("load to storage %v has no master hub", stgID)
		}
		if !factory.GetBuilder(stg).PublicStoreDesc().Enabled() {
			return nil, fmt.Errorf("load to storage %v has no public store", stgID)
		}

		loadToStgs[i] = stg
	}

	target := u.chooseUploadStorage(userStgs, affinity)

	// 给上传节点的IPFS加锁
	// TODO 考虑加Object的Create锁
	// 防止上传的副本被清除
	distMutex, err := reqbuilder.NewBuilder().Shard().Buzy(target.Storage.Storage.StorageID).MutexLock(u.distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire distlock: %w", err)
	}

	return &UpdateUploader{
		uploader:   u,
		pkgID:      pkgID,
		targetStg:  target.Storage,
		distMutex:  distMutex,
		loadToStgs: loadToStgs,
		loadToPath: loadToPath,
	}, nil
}

// chooseUploadStorage 选择一个上传文件的节点
// 1. 选择设置了亲和性的节点
// 2. 从与当前客户端相同地域的节点中随机选一个
// 3. 没有的话从所有节点选择延迟最低的节点
func (w *Uploader) chooseUploadStorage(storages []UploadStorageInfo, stgAffinity cdssdk.StorageID) UploadStorageInfo {
	if stgAffinity > 0 {
		aff, ok := lo.Find(storages, func(storage UploadStorageInfo) bool { return storage.Storage.Storage.StorageID == stgAffinity })
		if ok {
			return aff
		}
	}

	sameLocationStorages := lo.Filter(storages, func(e UploadStorageInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationStorages) > 0 {
		return sameLocationStorages[rand.Intn(len(sameLocationStorages))]
	}

	// 选择延迟最低的节点
	storages = sort2.Sort(storages, func(e1, e2 UploadStorageInfo) int { return sort2.Cmp(e1.Delay, e2.Delay) })

	return storages[0]
}

func (u *Uploader) BeginCreateLoad(userID cdssdk.UserID, bktID cdssdk.BucketID, pkgName string, loadTo []cdssdk.StorageID, loadToPath []string) (*CreateLoadUploader, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getStgs := u.stgMeta.GetMany(loadTo)

	targetStgs := make([]stgmod.StorageDetail, len(loadTo))
	for i, stg := range getStgs {
		if stg == nil {
			return nil, fmt.Errorf("storage %v not found", loadTo[i])
		}
		targetStgs[i] = *stg
	}

	createPkg, err := coorCli.CreatePackage(coormq.NewCreatePackage(userID, bktID, pkgName))
	if err != nil {
		return nil, fmt.Errorf("create package: %w", err)
	}

	reqBld := reqbuilder.NewBuilder()
	for _, stg := range targetStgs {
		reqBld.Shard().Buzy(stg.Storage.StorageID)
		reqBld.Storage().Buzy(stg.Storage.StorageID)
	}
	lock, err := reqBld.MutexLock(u.distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire distlock: %w", err)
	}

	return &CreateLoadUploader{
		pkg:        createPkg.Package,
		userID:     userID,
		targetStgs: targetStgs,
		loadRoots:  loadToPath,
		uploader:   u,
		distlock:   lock,
	}, nil
}

func (u *Uploader) UploadPart(userID cdssdk.UserID, objID cdssdk.ObjectID, index int, stream io.Reader) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	details, err := coorCli.GetObjectDetails(coormq.ReqGetObjectDetails([]cdssdk.ObjectID{objID}))
	if err != nil {
		return err
	}

	if details.Objects[0] == nil {
		return fmt.Errorf("object %v not found", objID)
	}

	objDe := details.Objects[0]
	_, ok := objDe.Object.Redundancy.(*cdssdk.MultipartUploadRedundancy)
	if !ok {
		return fmt.Errorf("object %v is not a multipart upload", objID)
	}

	var stg stgmod.StorageDetail
	if len(objDe.Blocks) > 0 {
		cstg := u.stgMeta.Get(objDe.Blocks[0].StorageID)
		if cstg == nil {
			return fmt.Errorf("storage %v not found", objDe.Blocks[0].StorageID)
		}

		stg = *cstg

	} else {
		getUserStgsResp, err := coorCli.GetUserStorageDetails(coormq.ReqGetUserStorageDetails(userID))
		if err != nil {
			return fmt.Errorf("getting user storages: %w", err)
		}

		cons := u.connectivity.GetAll()
		var userStgs []UploadStorageInfo
		for _, stg := range getUserStgsResp.Storages {
			if stg.MasterHub == nil {
				continue
			}

			delay := time.Duration(math.MaxInt64)

			con, ok := cons[stg.MasterHub.HubID]
			if ok && con.Latency != nil {
				delay = *con.Latency
			}

			userStgs = append(userStgs, UploadStorageInfo{
				Storage:        stg,
				Delay:          delay,
				IsSameLocation: stg.MasterHub.LocationID == stgglb.Local.LocationID,
			})
		}

		if len(userStgs) == 0 {
			return fmt.Errorf("user no available storages")
		}

		stg = u.chooseUploadStorage(userStgs, 0).Storage
	}

	lock, err := reqbuilder.NewBuilder().Shard().Buzy(stg.Storage.StorageID).MutexLock(u.distlock)
	if err != nil {
		return fmt.Errorf("acquire distlock: %w", err)
	}
	defer lock.Unlock()

	ft := ioswitch2.NewFromTo()
	fromDrv, hd := ioswitch2.NewFromDriver(ioswitch2.RawStream())
	ft.AddFrom(fromDrv).
		AddTo(ioswitch2.NewToShardStore(*stg.MasterHub, stg, ioswitch2.RawStream(), "shard"))

	plans := exec.NewPlanBuilder()
	err = parser.Parse(ft, plans)
	if err != nil {
		return fmt.Errorf("parse fromto: %w", err)
	}

	exeCtx := exec.NewExecContext()
	exec.SetValueByType(exeCtx, u.stgAgts)
	exec := plans.Execute(exeCtx)
	exec.BeginWrite(io.NopCloser(stream), hd)
	ret, err := exec.Wait(context.TODO())
	if err != nil {
		return fmt.Errorf("executing plan: %w", err)
	}

	shardInfo := ret["shard"].(*ops2.ShardInfoValue)
	_, err = coorCli.AddMultipartUploadPart(coormq.ReqAddMultipartUploadPart(userID, objID, stgmod.ObjectBlock{
		ObjectID:  objID,
		Index:     index,
		StorageID: stg.Storage.StorageID,
		FileHash:  shardInfo.Hash,
		Size:      shardInfo.Size,
	}))
	return err
}
