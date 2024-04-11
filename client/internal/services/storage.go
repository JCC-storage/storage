package services

import (
	"fmt"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// StorageService 存储服务结构体，继承自Service结构体
type StorageService struct {
	*Service
}

// StorageSvc 返回StorageService的实例
func (svc *Service) StorageSvc() *StorageService {
	return &StorageService{Service: svc}
}

// StartStorageLoadPackage 开始加载存储包。
// userID: 用户ID，用于标识请求的用户。
// packageID: 包ID，用于标识需要加载的数据包。
// storageID: 存储ID，用于标识数据存储的位置。
// 返回值1: 节点ID，标识进行存储操作的节点。
// 返回值2: 任务ID，标识加载数据包的任务。
// 返回值3: 错误，如果执行过程中出现错误，则返回错误信息。
func (svc *StorageService) StartStorageLoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID) (cdssdk.NodeID, string, error) {
	// 获取协调器MQ客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return 0, "", fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 从协调器获取存储信息
	stgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(userID, storageID))
	if err != nil {
		return 0, "", fmt.Errorf("getting storage info: %w", err)
	}

	// 获取代理MQ客户端
	agentCli, err := stgglb.AgentMQPool.Acquire(stgResp.NodeID)
	if err != nil {
		return 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	// 向代理发送开始加载存储包的请求
	startResp, err := agentCli.StartStorageLoadPackage(agtmq.NewStartStorageLoadPackage(userID, packageID, storageID))
	if err != nil {
		return 0, "", fmt.Errorf("start storage load package: %w", err)
	}

	return stgResp.NodeID, startResp.TaskID, nil
}

/*
WaitStorageLoadPackage 等待存储包加载完成。
参数：
- nodeID：节点ID
- taskID：任务ID
- waitTimeout：等待超时时间
返回值：
- bool：任务是否完成
- string：错误信息
- error：错误信息
*/
func (svc *StorageService) WaitStorageLoadPackage(nodeID cdssdk.NodeID, taskID string, waitTimeout time.Duration) (bool, string, error) {
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		// TODO 失败是否要当做任务已经结束？
		return true, "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	waitResp, err := agentCli.WaitStorageLoadPackage(agtmq.NewWaitStorageLoadPackage(taskID, waitTimeout.Milliseconds()))
	if err != nil {
		// TODO 请求失败是否要当做任务已经结束？
		return true, "", fmt.Errorf("wait storage load package: %w", err)
	}

	if !waitResp.IsComplete {
		return false, "", nil
	}

	if waitResp.Error != "" {
		return true, "", fmt.Errorf("%s", waitResp.Error)
	}

	return true, waitResp.FullPath, nil
}

// DeleteStoragePackage 删除存储包的函数，当前未实现。
func (svc *StorageService) DeleteStoragePackage(userID int64, packageID int64, storageID int64) error {
	// TODO
	panic("not implement yet")
}

/*
StartStorageCreatePackage 请求节点启动从Storage中上传文件的任务。
参数：
- userID：用户ID
- bucketID：存储桶ID
- name：文件名
- storageID：存储ID
- path：文件路径
- nodeAffinity：节点亲和性（可选）
返回值：
- cdssdk.NodeID：节点ID
- string：任务ID
- error：错误信息
*/
func (svc *StorageService) StartStorageCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, storageID cdssdk.StorageID, path string, nodeAffinity *cdssdk.NodeID) (cdssdk.NodeID, string, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return 0, "", fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	stgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(userID, storageID))
	if err != nil {
		return 0, "", fmt.Errorf("getting storage info: %w", err)
	}

	agentCli, err := stgglb.AgentMQPool.Acquire(stgResp.NodeID)
	if err != nil {
		return 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	startResp, err := agentCli.StartStorageCreatePackage(agtmq.NewStartStorageCreatePackage(userID, bucketID, name, storageID, path, nodeAffinity))
	if err != nil {
		return 0, "", fmt.Errorf("start storage upload package: %w", err)
	}

	return stgResp.NodeID, startResp.TaskID, nil
}

/*
WaitStorageCreatePackage 等待存储包创建完成。
参数：
- nodeID：节点ID
- taskID：任务ID
- waitTimeout：等待超时时间
返回值：
- bool：任务是否完成
- cdssdk.PackageID：包ID
- error：错误信息
*/
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

/*
GetInfo 获取存储信息。
参数：
- userID：用户ID
- storageID：存储ID
返回值：
-
*/
func (svc *StorageService) GetInfo(userID cdssdk.UserID, storageID cdssdk.StorageID) (*model.Storage, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(userID, storageID))
	if err != nil {
		return nil, fmt.Errorf("request to coordinator: %w", err)
	}

	return &getResp.Storage, nil
}
