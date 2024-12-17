package services

import (
	"fmt"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	datamapmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/datamap"
)

// HubService 是关于节点操作的服务结构体
type HubService struct {
	*Service
}

// HubSvc 创建并返回一个HubService的实例
func (svc *Service) HubSvc() *HubService {
	return &HubService{Service: svc}
}

// GetHubs 根据提供的节点ID列表，获取对应的节点信息
// 参数:
//
//	hubIDs []cdssdk.HubID - 需要查询的节点ID列表
//
// 返回值:
//
//	[]cdssdk.Hub - 获取到的节点信息列表
//	error - 如果过程中发生错误，则返回错误信息
func (svc *HubService) GetHubs(hubIDs []cdssdk.HubID) ([]cdssdk.Hub, error) {
	// 从协调器MQ池中获取一个客户端实例
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	// 确保在函数结束时释放客户端实例回池
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器发送获取节点信息的请求
	getResp, err := coorCli.GetHubs(coormq.NewGetHubs(hubIDs))
	if err != nil {
		return nil, fmt.Errorf("requesting to coordinator: %w", err)
	}

	// 返回获取到的节点信息
	return getResp.Hubs, nil
}

// GetHubStat 根据提供的节点ID列表,获取中心的总数据量

func (svc *HubService) GetHubStat(hubID cdssdk.HubID) (stgmod.HubStat, error) {
	datamapCli, err := stgglb.DatamapMQPool.Acquire()
	if err != nil {
		return stgmod.HubStat{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.DatamapMQPool.Release(datamapCli)

	getResp, err := datamapCli.GetHubStat(datamapmq.NewGetHubStat(hubID))
	if err != nil {
		return stgmod.HubStat{}, fmt.Errorf("requesting to coordinator: %w", err)
	}

	return getResp.HubStat, nil
}
