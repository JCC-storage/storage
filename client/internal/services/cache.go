package services

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// CacheService 缓存服务结构体，继承自Service。
type CacheService struct {
	*Service
}

// CacheSvc 创建并返回一个CacheService的实例。
func (svc *Service) CacheSvc() *CacheService {
	return &CacheService{Service: svc}
}

// StartCacheMovePackage 启动缓存移动包的流程。
// userID: 用户标识符；
// packageID: 包标识符；
// nodeID: 节点标识符；
// 返回任务ID和可能的错误。
func (svc *CacheService) StartCacheMovePackage(userID cdssdk.UserID, packageID cdssdk.PackageID, nodeID cdssdk.NodeID) (string, error) {
	// 获取Agent消息队列客户端
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	// 向Agent发起启动缓存移动包的请求
	startResp, err := agentCli.StartCacheMovePackage(agtmq.NewStartCacheMovePackage(userID, packageID))
	if err != nil {
		return "", fmt.Errorf("start cache move package: %w", err)
	}

	return startResp.TaskID, nil
}

// WaitCacheMovePackage 等待缓存移动包完成。
// nodeID: 节点标识符；
// taskID: 任务标识符；
// waitTimeout: 等待超时时间；
// 返回任务是否完成和可能的错误。
func (svc *CacheService) WaitCacheMovePackage(nodeID cdssdk.NodeID, taskID string, waitTimeout time.Duration) (bool, error) {
	// 获取Agent消息队列客户端
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return true, fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	// 向Agent查询缓存移动包状态
	waitResp, err := agentCli.WaitCacheMovePackage(agtmq.NewWaitCacheMovePackage(taskID, waitTimeout.Milliseconds()))
	if err != nil {
		return true, fmt.Errorf("wait cache move package: %w", err)
	}

	if !waitResp.IsComplete {
		return false, nil
	}

	if waitResp.Error != "" {
		return true, fmt.Errorf("%s", waitResp.Error)
	}

	return true, nil
}

// CacheRemovePackage 请求移除缓存包。
// packageID: 包标识符；
// nodeID: 节点标识符；
// 返回可能的错误。
func (svc *CacheService) CacheRemovePackage(packageID cdssdk.PackageID, nodeID cdssdk.NodeID) error {
	// 获取协调器消息队列客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器发送移除缓存包的请求
	_, err = coorCli.CacheRemovePackage(coormq.ReqCacheRemoveMovedPackage(packageID, nodeID))
	if err != nil {
		return fmt.Errorf("requesting to coordinator: %w", err)
	}

	return nil
}
