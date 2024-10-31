package services

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
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

func (svc *StorageService) StartStorageLoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID) (cdssdk.NodeID, string, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return 0, "", fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	stgResp, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{storageID}))
	if err != nil {
		return 0, "", fmt.Errorf("getting storage info: %w", err)
	}

	if stgResp.Storages[0].Shard == nil {
		return 0, "", fmt.Errorf("shard storage is not enabled")
	}

	agentCli, err := stgglb.AgentMQPool.Acquire(stgResp.Storages[0].MasterHub.NodeID)
	if err != nil {
		return 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	startResp, err := agentCli.StartStorageLoadPackage(agtmq.NewStartStorageLoadPackage(userID, packageID, storageID))
	if err != nil {
		return 0, "", fmt.Errorf("start storage load package: %w", err)
	}

	return stgResp.Storages[0].MasterHub.NodeID, startResp.TaskID, nil
}

type StorageLoadPackageResult struct {
	PackagePath string
	LocalBase   string
	RemoteBase  string
}

func (svc *StorageService) WaitStorageLoadPackage(nodeID cdssdk.NodeID, taskID string, waitTimeout time.Duration) (bool, *StorageLoadPackageResult, error) {
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		// TODO 失败是否要当做任务已经结束？
		return true, nil, fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	waitResp, err := agentCli.WaitStorageLoadPackage(agtmq.NewWaitStorageLoadPackage(taskID, waitTimeout.Milliseconds()))
	if err != nil {
		// TODO 请求失败是否要当做任务已经结束？
		return true, nil, fmt.Errorf("wait storage load package: %w", err)
	}

	if !waitResp.IsComplete {
		return false, nil, nil
	}

	if waitResp.Error != "" {
		return true, nil, fmt.Errorf("%s", waitResp.Error)
	}

	return true, &StorageLoadPackageResult{
		PackagePath: waitResp.PackagePath,
		LocalBase:   waitResp.LocalBase,
		RemoteBase:  waitResp.RemoteBase,
	}, nil
}

func (svc *StorageService) DeleteStoragePackage(userID int64, packageID int64, storageID int64) error {
	// TODO
	panic("not implement yet")
}

// 请求节点启动从Storage中上传文件的任务。会返回节点ID和任务ID
func (svc *StorageService) StartStorageCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, storageID cdssdk.StorageID, path string, nodeAffinity *cdssdk.NodeID) (cdssdk.NodeID, string, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return 0, "", fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	stgResp, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{storageID}))
	if err != nil {
		return 0, "", fmt.Errorf("getting storage info: %w", err)
	}

	if stgResp.Storages[0].Shard == nil {
		return 0, "", fmt.Errorf("shard storage is not enabled")
	}

	agentCli, err := stgglb.AgentMQPool.Acquire(stgResp.Storages[0].MasterHub.NodeID)
	if err != nil {
		return 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	startResp, err := agentCli.StartStorageCreatePackage(agtmq.NewStartStorageCreatePackage(userID, bucketID, name, storageID, path, nodeAffinity))
	if err != nil {
		return 0, "", fmt.Errorf("start storage upload package: %w", err)
	}

	return stgResp.Storages[0].MasterHub.NodeID, startResp.TaskID, nil
}

func (svc *StorageService) WaitStorageCreatePackage(nodeID cdssdk.NodeID, taskID string, waitTimeout time.Duration) (bool, cdssdk.PackageID, error) {
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
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
