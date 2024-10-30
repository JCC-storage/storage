package mq

import (
	"database/sql"
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gorm.io/gorm"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetStorage(msg *coormq.GetStorage) (*coormq.GetStorageResp, *mq.CodeMessage) {
	stg, err := svc.db2.Storage().GetUserStorage(svc.db2.DefCtx(), msg.UserID, msg.StorageID)
	if err != nil {
		logger.Warnf("getting user storage: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get user storage failed")
	}

	return mq.ReplyOK(coormq.RespGetStorage(stg))
}

func (svc *Service) GetStorageDetails(msg *coormq.GetStorageDetails) (*coormq.GetStorageDetailsResp, *mq.CodeMessage) {
	stgsMp := make(map[cdssdk.StorageID]*stgmod.StorageDetail)

	svc.db2.DoTx(func(tx db2.SQLContext) error {
		stgs, err := svc.db2.Storage().BatchGetByID(tx, msg.StorageIDs)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting storage: %w", err)
		}
		var masterHubIDs []cdssdk.NodeID
		for _, stg := range stgs {
			stgsMp[stg.StorageID] = &stgmod.StorageDetail{
				Storage: stg,
			}
			masterHubIDs = append(masterHubIDs, stg.MasterHub)
		}

		// 获取监护Hub信息
		masterHubs, err := svc.db2.Node().BatchGetByID(tx, masterHubIDs)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting master hub: %w", err)
		}
		masterHubMap := make(map[cdssdk.NodeID]cdssdk.Node)
		for _, hub := range masterHubs {
			masterHubMap[hub.NodeID] = hub
		}

		// 获取分片存储
		shards, err := svc.db2.ShardStorage().BatchGetByStorageIDs(tx, msg.StorageIDs)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting shard storage: %w", err)
		}
		for _, shard := range shards {
			stgsMp[shard.StorageID].Shard = &shard
		}

		for _, stg := range stgsMp {
			if stg.Shard != nil {
				hub := masterHubMap[stg.MasterHub.NodeID]
				stg.MasterHub = &hub
			}
		}

		// 获取共享存储的相关信息
		shareds, err := svc.db2.SharedStorage().BatchGetByStorageIDs(tx, msg.StorageIDs)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting shared storage: %w", err)
		}
		for _, shared := range shareds {
			stgsMp[shared.StorageID].Shared = &shared
		}

		return nil
	})

	ret := make([]*stgmod.StorageDetail, len(msg.StorageIDs))
	for i, id := range msg.StorageIDs {
		stg, ok := stgsMp[id]
		if !ok {
			ret[i] = nil
			continue
		}
		ret[i] = stg
	}

	return mq.ReplyOK(coormq.RespGetStorageDetails(ret))
}

func (svc *Service) GetUserStorageDetails(msg *coormq.GetUserStorageDetails) (*coormq.GetUserStorageDetailsResp, *mq.CodeMessage) {
	stgsMp := make(map[cdssdk.StorageID]*stgmod.StorageDetail)

	svc.db2.DoTx(func(tx db2.SQLContext) error {
		stgs, err := svc.db2.Storage().GetUserStorages(tx, msg.UserID)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting user storages: %w", err)
		}
		var masterHubIDs []cdssdk.NodeID
		for _, stg := range stgs {
			stgsMp[stg.StorageID] = &stgmod.StorageDetail{
				Storage: stg,
			}
			masterHubIDs = append(masterHubIDs, stg.MasterHub)
		}

		// 监护Hub的信息
		masterHubs, err := svc.db2.Node().BatchGetByID(tx, masterHubIDs)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting master hub: %w", err)
		}
		masterHubMap := make(map[cdssdk.NodeID]cdssdk.Node)
		for _, hub := range masterHubs {
			masterHubMap[hub.NodeID] = hub
		}

		stgIDs := lo.Map(stgs, func(stg cdssdk.Storage, i int) cdssdk.StorageID { return stg.StorageID })

		// 获取分片存储信息
		shards, err := svc.db2.ShardStorage().BatchGetByStorageIDs(tx, stgIDs)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting shard storage: %w", err)
		}
		for _, shard := range shards {
			stgsMp[shard.StorageID].Shard = &shard
		}
		for _, stg := range stgsMp {
			if stg.Shard != nil {
				hub := masterHubMap[stg.MasterHub.NodeID]
				stg.MasterHub = &hub
			}
		}

		// 获取共享存储的相关信息
		shareds, err := svc.db2.SharedStorage().BatchGetByStorageIDs(tx, stgIDs)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting shared storage: %w", err)
		}
		for _, shared := range shareds {
			stgsMp[shared.StorageID].Shared = &shared
		}

		return nil
	})

	var ret []stgmod.StorageDetail
	for _, id := range stgsMp {
		ret = append(ret, *id)
	}

	return mq.ReplyOK(coormq.RespGetUserStorageDetails(ret))
}

func (svc *Service) GetStorageByName(msg *coormq.GetStorageByName) (*coormq.GetStorageByNameResp, *mq.CodeMessage) {
	stg, err := svc.db2.Storage().GetUserStorageByName(svc.db2.DefCtx(), msg.UserID, msg.Name)
	if err != nil {
		logger.Warnf("getting user storage by name: %s", err.Error())

		if err == sql.ErrNoRows {
			return nil, mq.Failed(errorcode.DataNotFound, "storage not found")
		}

		return nil, mq.Failed(errorcode.OperationFailed, "get user storage failed")
	}

	return mq.ReplyOK(coormq.RespGetStorageByNameResp(stg))
}

func (svc *Service) StoragePackageLoaded(msg *coormq.StoragePackageLoaded) (*coormq.StoragePackageLoadedResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		// 可以不用检查用户是否存在
		if ok, _ := svc.db2.Package().IsAvailable(tx, msg.UserID, msg.PackageID); !ok {
			return fmt.Errorf("package is not available to user")
		}

		if ok, _ := svc.db2.Storage().IsAvailable(tx, msg.UserID, msg.StorageID); !ok {
			return fmt.Errorf("storage is not available to user")
		}

		err := svc.db2.StoragePackage().CreateOrUpdate(tx, msg.StorageID, msg.PackageID, msg.UserID)
		if err != nil {
			return fmt.Errorf("creating storage package: %w", err)
		}

		stg, err := svc.db2.Storage().GetByID(tx, msg.StorageID)
		if err != nil {
			return fmt.Errorf("getting storage: %w", err)
		}

		err = svc.db2.PinnedObject().CreateFromPackage(tx, msg.PackageID, stg.StorageID)
		if err != nil {
			return fmt.Errorf("creating pinned object from package: %w", err)
		}

		if len(msg.PinnedBlocks) > 0 {
			err = svc.db2.ObjectBlock().BatchCreate(tx, msg.PinnedBlocks)
			if err != nil {
				return fmt.Errorf("batch creating object block: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("StorageID", msg.StorageID).
			WithField("PackageID", msg.PackageID).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "user load package to storage failed")
	}

	return mq.ReplyOK(coormq.NewStoragePackageLoadedResp())
}
