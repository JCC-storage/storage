package services

import (
	"context"
	"fmt"
	"path"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader/strategy"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
)

type StorageService struct {
	*Service
}

func (svc *Service) StorageSvc() *StorageService {
	return &StorageService{Service: svc}
}

func (svc *StorageService) Get(userID cdssdk.UserID, storageID cdssdk.StorageID) (*model.Storage, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetStorage(coormq.ReqGetStorage(userID, storageID))
	if err != nil {
		return nil, fmt.Errorf("request to coordinator: %w", err)
	}

	return &getResp.Storage, nil
}

func (svc *StorageService) GetByName(userID cdssdk.UserID, name string) (*model.Storage, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetStorageByName(coormq.ReqGetStorageByName(userID, name))
	if err != nil {
		return nil, fmt.Errorf("request to coordinator: %w", err)
	}

	return &getResp.Storage, nil
}

func (svc *StorageService) GetDetails(stgIDs []cdssdk.StorageID) ([]*stgmod.StorageDetail, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails(stgIDs))
	if err != nil {
		return nil, fmt.Errorf("request to coordinator: %w", err)
	}

	return getResp.Storages, nil
}

func (svc *StorageService) LoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID, rootPath string) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	destStg := svc.StorageMeta.Get(storageID)
	if destStg == nil {
		return fmt.Errorf("storage not found: %d", storageID)
	}
	if destStg.MasterHub == nil {
		return fmt.Errorf("storage %v has no master hub", storageID)
	}

	details, err := coorCli.GetPackageObjectDetails(coormq.ReqGetPackageObjectDetails(packageID))
	if err != nil {
		return err
	}

	var pinned []cdssdk.ObjectID
	plans := exec.NewPlanBuilder()
	for _, obj := range details.Objects {
		strg, err := svc.StrategySelector.Select(strategy.Request{
			Detail:  obj,
			DestHub: destStg.MasterHub.HubID,
		})
		if err != nil {
			return fmt.Errorf("select download strategy: %w", err)
		}

		ft := ioswitch2.NewFromTo()
		switch strg := strg.(type) {
		case *strategy.DirectStrategy:
			ft.AddFrom(ioswitch2.NewFromShardstore(strg.Detail.Object.FileHash, *strg.Storage.MasterHub, strg.Storage, ioswitch2.RawStream()))

		case *strategy.ECReconstructStrategy:
			for i, b := range strg.Blocks {
				ft.AddFrom(ioswitch2.NewFromShardstore(b.FileHash, *strg.Storages[i].MasterHub, strg.Storages[i], ioswitch2.ECStream(b.Index)))
				ft.ECParam = &strg.Redundancy
			}
		default:
			return fmt.Errorf("unsupported download strategy: %T", strg)
		}

		ft.AddTo(ioswitch2.NewLoadToPublic(*destStg.MasterHub, *destStg, path.Join(rootPath, obj.Object.Path)))
		// 顺便保存到同存储服务的分片存储中
		if factory.GetBuilder(*destStg).ShardStoreDesc().Enabled() {
			ft.AddTo(ioswitch2.NewToShardStore(*destStg.MasterHub, *destStg, ioswitch2.RawStream(), ""))
			pinned = append(pinned, obj.Object.ObjectID)
		}

		err = parser.Parse(ft, plans)
		if err != nil {
			return fmt.Errorf("parse plan: %w", err)
		}
	}

	mutex, err := reqbuilder.NewBuilder().
		// 保护在storage目录中下载的文件
		Storage().Buzy(storageID).
		// 保护下载文件时同时保存到IPFS的文件
		Shard().Buzy(storageID).
		MutexLock(svc.DistLock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}

	// 记录访问统计
	for _, obj := range details.Objects {
		svc.AccessStat.AddAccessCounter(obj.Object.ObjectID, packageID, storageID, 1)
	}

	defer mutex.Unlock()

	drv := plans.Execute(exec.NewExecContext())
	_, err = drv.Wait(context.Background())
	if err != nil {
		return err
	}

	// 失败也没关系
	coorCli.StoragePackageLoaded(coormq.ReqStoragePackageLoaded(userID, storageID, packageID, rootPath, pinned))
	return nil
}

// 请求节点启动从Storage中上传文件的任务。会返回节点ID和任务ID
func (svc *StorageService) StartStorageCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, storageID cdssdk.StorageID, path string, storageAffinity cdssdk.StorageID) (cdssdk.HubID, string, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return 0, "", fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	stgResp, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{storageID}))
	if err != nil {
		return 0, "", fmt.Errorf("getting storage info: %w", err)
	}

	if stgResp.Storages[0].Storage.ShardStore == nil {
		return 0, "", fmt.Errorf("shard storage is not enabled")
	}

	agentCli, err := stgglb.AgentMQPool.Acquire(stgResp.Storages[0].MasterHub.HubID)
	if err != nil {
		return 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	startResp, err := agentCli.StartStorageCreatePackage(agtmq.NewStartStorageCreatePackage(userID, bucketID, name, storageID, path, storageAffinity))
	if err != nil {
		return 0, "", fmt.Errorf("start storage upload package: %w", err)
	}

	return stgResp.Storages[0].MasterHub.HubID, startResp.TaskID, nil
}

func (svc *StorageService) WaitStorageCreatePackage(hubID cdssdk.HubID, taskID string, waitTimeout time.Duration) (bool, cdssdk.PackageID, error) {
	agentCli, err := stgglb.AgentMQPool.Acquire(hubID)
	if err != nil {
		// TODO 失败是否要当做任务已经结束？
		return true, 0, fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	waitResp, err := agentCli.WaitStorageCreatePackage(agtmq.NewWaitStorageCreatePackage(taskID, waitTimeout.Milliseconds()))
	if err != nil {
		// TODO 请求失败是否要当做任务已经结束？
		return true, 0, fmt.Errorf("wait storage upload package: %w", err)
	}

	if !waitResp.IsComplete {
		return false, 0, nil
	}

	if waitResp.Error != "" {
		return true, 0, fmt.Errorf("%s", waitResp.Error)
	}

	return true, waitResp.PackageID, nil
}
