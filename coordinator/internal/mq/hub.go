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

	hub, err := svc.db2.Hub().GetByID(svc.db2.DefCtx(), msg.HubID)
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

	for _, stg := range stgs {
		detailsMap[stg.StorageID] = &stgmod.StorageDetail{
			Storage:   stg,
			MasterHub: &hub,
		}
	}

	var details []stgmod.StorageDetail
	for _, detail := range detailsMap {
		details = append(details, *detail)
	}

	return mq.ReplyOK(coormq.RespGetHubConfig(hub, details))
}

func (svc *Service) GetUserHubs(msg *coormq.GetUserHubs) (*coormq.GetUserHubsResp, *mq.CodeMessage) {
	hubs, err := svc.db2.Hub().GetUserHubs(svc.db2.DefCtx(), msg.UserID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("query user hubs failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "query user hubs failed")
	}

	return mq.ReplyOK(coormq.NewGetUserHubsResp(hubs))
}

func (svc *Service) GetHubs(msg *coormq.GetHubs) (*coormq.GetHubsResp, *mq.CodeMessage) {
	var hubs []cdssdk.Hub

	if msg.HubIDs == nil {
		var err error
		hubs, err = svc.db2.Hub().GetAllHubs(svc.db2.DefCtx())
		if err != nil {
			logger.Warnf("getting all hubs: %s", err.Error())
			return nil, mq.Failed(errorcode.OperationFailed, "get all hub failed")
		}

	} else {
		// 可以不用事务
		for _, id := range msg.HubIDs {
			hub, err := svc.db2.Hub().GetByID(svc.db2.DefCtx(), id)
			if err != nil {
				logger.WithField("HubID", id).
					Warnf("query hub failed, err: %s", err.Error())
				return nil, mq.Failed(errorcode.OperationFailed, "query hub failed")
			}

			hubs = append(hubs, hub)
		}
	}

	return mq.ReplyOK(coormq.NewGetHubsResp(hubs))
}

func (svc *Service) GetHubConnectivities(msg *coormq.GetHubConnectivities) (*coormq.GetHubConnectivitiesResp, *mq.CodeMessage) {
	cons, err := svc.db2.HubConnectivity().BatchGetByFromHub(svc.db2.DefCtx(), msg.HubIDs)
	if err != nil {
		logger.Warnf("batch get hub connectivities by from hub: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch get hub connectivities by from hub failed")
	}

	return mq.ReplyOK(coormq.RespGetHubConnectivities(cons))
}

func (svc *Service) UpdateHubConnectivities(msg *coormq.UpdateHubConnectivities) (*coormq.UpdateHubConnectivitiesResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		// 只有发起节点和目的节点都存在，才能插入这条记录到数据库
		allHubs, err := svc.db2.Hub().GetAllHubs(tx)
		if err != nil {
			return fmt.Errorf("getting all hubs: %w", err)
		}

		allHubID := make(map[cdssdk.HubID]bool)
		for _, hub := range allHubs {
			allHubID[hub.HubID] = true
		}

		var avaiCons []cdssdk.HubConnectivity
		for _, con := range msg.Connectivities {
			if allHubID[con.FromHubID] && allHubID[con.ToHubID] {
				avaiCons = append(avaiCons, con)
			}
		}

		err = svc.db2.HubConnectivity().BatchUpdateOrCreate(tx, avaiCons)
		if err != nil {
			return fmt.Errorf("batch update or create hub connectivities: %s", err)
		}

		return nil
	})
	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(coormq.RespUpdateHubConnectivities())
}
