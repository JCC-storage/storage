package uploader

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/samber/lo"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/svcmgr"
)

type Uploader struct {
	distlock     *distlock.Service
	connectivity *connectivity.Collector
	stgMgr       *svcmgr.Manager
}

func NewUploader(distlock *distlock.Service, connectivity *connectivity.Collector, stgMgr *svcmgr.Manager) *Uploader {
	return &Uploader{
		distlock:     distlock,
		connectivity: connectivity,
		stgMgr:       stgMgr,
	}
}

func (u *Uploader) BeginUpdate(userID cdssdk.UserID, pkgID cdssdk.PackageID, affinity cdssdk.StorageID) (*UpdateUploader, error) {
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
		if ok && con.Delay != nil {
			delay = *con.Delay
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

	target := u.chooseUploadStorage(userStgs, affinity)

	// 给上传节点的IPFS加锁
	// TODO 考虑加Object的Create锁
	// 防止上传的副本被清除
	distMutex, err := reqbuilder.NewBuilder().Shard().Buzy(target.Storage.Storage.StorageID).MutexLock(u.distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire distlock: %w", err)
	}

	return &UpdateUploader{
		uploader:  u,
		pkgID:     pkgID,
		targetStg: target.Storage,
		distMutex: distMutex,
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

func (u *Uploader) BeginCreateLoad(userID cdssdk.UserID, bktID cdssdk.BucketID, pkgName string, loadTo []cdssdk.StorageID) (*CreateLoadUploader, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getStgs, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails(loadTo))
	if err != nil {
		return nil, fmt.Errorf("getting storages: %w", err)
	}

	targetStgs := make([]stgmod.StorageDetail, len(loadTo))
	for i, stg := range getStgs.Storages {
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
		reqBld.Metadata().StoragePackage().CreateOne(userID, stg.Storage.StorageID, createPkg.Package.PackageID)
	}
	lock, err := reqBld.MutexLock(u.distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire distlock: %w", err)
	}

	return &CreateLoadUploader{
		pkg:        createPkg.Package,
		userID:     userID,
		targetStgs: targetStgs,
		uploader:   u,
		distlock:   lock,
	}, nil
}
