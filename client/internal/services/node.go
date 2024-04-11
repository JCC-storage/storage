package services

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// NodeService 是关于节点操作的服务结构体
type NodeService struct {
	*Service
}

// NodeSvc 创建并返回一个NodeService的实例
func (svc *Service) NodeSvc() *NodeService {
	return &NodeService{Service: svc}
}

// GetNodes 根据提供的节点ID列表，获取对应的节点信息
// 参数:
//
//	nodeIDs []cdssdk.NodeID - 需要查询的节点ID列表
//
// 返回值:
//
//	[]cdssdk.Node - 获取到的节点信息列表
//	error - 如果过程中发生错误，则返回错误信息
func (svc *NodeService) GetNodes(nodeIDs []cdssdk.NodeID) ([]cdssdk.Node, error) {
	// 从协调器MQ池中获取一个客户端实例
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	// 确保在函数结束时释放客户端实例回池
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器发送获取节点信息的请求
	getResp, err := coorCli.GetNodes(coormq.NewGetNodes(nodeIDs))
	if err != nil {
		return nil, fmt.Errorf("requesting to coordinator: %w", err)
	}

	// 返回获取到的节点信息
	return getResp.Nodes, nil
}
