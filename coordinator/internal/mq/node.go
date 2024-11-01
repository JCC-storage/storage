package mq

import (
	"fmt"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetHubConfig(msg *coormq.GetHubConfig) (*coormq.GetHubConfigResp, *mq.CodeMessage) {
	log := logger.WithField("HubID", msg.HubID)

	hub, err := svc.db2.Node().GetByID(svc.db2.DefCtx(), msg.HubID)
	if err != nil {
		log.Warnf("getting hub: %v", err)
		return nil, mq.Failed(errorcode.OperationFailed, fmt.Sprintf("getting hub: %v", err))
	}

	detailsMap := make(map[cdssdk.StorageID]*stgmod.StorageDetail)

	stgs, err := svc.db2.Storage().GetHubStorages(svc.db2.DefCtx(), msg.HubID)
	if err != nil {
		log.Warnf("getting hub storages: %v", err)
		return nil, mq.Failed(errorcode.OperationFailed, fmt.Sprintf("getting hub storages: %v", err))
	}

	var stgIDs []cdssdk.StorageID
	for _, stg := range stgs {
		detailsMap[stg.StorageID] = &stgmod.StorageDetail{
			Storage:   stg,
			MasterHub: &hub,
		}
		stgIDs = append(stgIDs, stg.StorageID)
	}

	shards, err := svc.db2.ShardStorage().BatchGetByStorageIDs(svc.db2.DefCtx(), stgIDs)
	if err != nil {
		log.Warnf("getting shard storages: %v", err)
		return nil, mq.Failed(errorcode.OperationFailed, fmt.Sprintf("getting shard storages: %v", err))
	}
	for _, shard := range shards {
		sh := shard
		detailsMap[shard.StorageID].Shard = &sh
	}

	shareds, err := svc.db2.SharedStorage().BatchGetByStorageIDs(svc.db2.DefCtx(), stgIDs)
	if err != nil {
		log.Warnf("getting shared storages: %v", err)
		return nil, mq.Failed(errorcode.OperationFailed, fmt.Sprintf("getting shared storages: %v", err))
	}
	for _, shared := range shareds {
		sh := shared
		detailsMap[shared.StorageID].Shared = &sh
	}

	var details []stgmod.StorageDetail
	for _, detail := range detailsMap {
		details = append(details, *detail)
	}

	return mq.ReplyOK(coormq.RespGetHubConfig(hub, details))
}

func (svc *Service) GetUserNodes(msg *coormq.GetUserNodes) (*coormq.GetUserNodesResp, *mq.CodeMessage) {
	nodes, err := svc.db2.Node().GetUserNodes(svc.db2.DefCtx(), msg.UserID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("query user nodes failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "query user nodes failed")
	}

	return mq.ReplyOK(coormq.NewGetUserNodesResp(nodes))
}

func (svc *Service) GetNodes(msg *coormq.GetNodes) (*coormq.GetNodesResp, *mq.CodeMessage) {
	var nodes []cdssdk.Node

	if msg.NodeIDs == nil {
		var err error
		nodes, err = svc.db2.Node().GetAllNodes(svc.db2.DefCtx())
		if err != nil {
			logger.Warnf("getting all nodes: %s", err.Error())
			return nil, mq.Failed(errorcode.OperationFailed, "get all node failed")
		}

	} else {
		// 可以不用事务
		for _, id := range msg.NodeIDs {
			node, err := svc.db2.Node().GetByID(svc.db2.DefCtx(), id)
			if err != nil {
				logger.WithField("NodeID", id).
					Warnf("query node failed, err: %s", err.Error())
				return nil, mq.Failed(errorcode.OperationFailed, "query node failed")
			}

			nodes = append(nodes, node)
		}
	}

	return mq.ReplyOK(coormq.NewGetNodesResp(nodes))
}

func (svc *Service) GetNodeConnectivities(msg *coormq.GetNodeConnectivities) (*coormq.GetNodeConnectivitiesResp, *mq.CodeMessage) {
	cons, err := svc.db2.NodeConnectivity().BatchGetByFromNode(svc.db2.DefCtx(), msg.NodeIDs)
	if err != nil {
		logger.Warnf("batch get node connectivities by from node: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch get node connectivities by from node failed")
	}

	return mq.ReplyOK(coormq.RespGetNodeConnectivities(cons))
}

func (svc *Service) UpdateNodeConnectivities(msg *coormq.UpdateNodeConnectivities) (*coormq.UpdateNodeConnectivitiesResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		// 只有发起节点和目的节点都存在，才能插入这条记录到数据库
		allNodes, err := svc.db2.Node().GetAllNodes(tx)
		if err != nil {
			return fmt.Errorf("getting all nodes: %w", err)
		}

		allNodeID := make(map[cdssdk.NodeID]bool)
		for _, node := range allNodes {
			allNodeID[node.NodeID] = true
		}

		var avaiCons []cdssdk.NodeConnectivity
		for _, con := range msg.Connectivities {
			if allNodeID[con.FromNodeID] && allNodeID[con.ToNodeID] {
				avaiCons = append(avaiCons, con)
			}
		}

		err = svc.db2.NodeConnectivity().BatchUpdateOrCreate(tx, avaiCons)
		if err != nil {
			return fmt.Errorf("batch update or create node connectivities: %s", err)
		}

		return nil
	})
	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(coormq.RespUpdateNodeConnectivities())
}
