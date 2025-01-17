package mq

import (
	"errors"
	"fmt"
	"time"

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

		details := make([]stgmod.StorageDetail, len(stgs))
		for i, stg := range stgs {
			details[i] = stgmod.StorageDetail{
				Storage: stg,
			}
			stgsMp[stg.StorageID] = &details[i]
		}
		err = svc.db2.Storage().FillDetails(tx, details)
		if err != nil {
			return err
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
	var ret []stgmod.StorageDetail

	svc.db2.DoTx(func(tx db2.SQLContext) error {
		stgs, err := svc.db2.Storage().GetUserStorages(tx, msg.UserID)
		if err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("getting user storages: %w", err)
		}

		for _, stg := range stgs {
			ret = append(ret, stgmod.StorageDetail{
				Storage: stg,
			})
		}
		err = svc.db2.Storage().FillDetails(tx, ret)
		if err != nil {
			return err
		}

		return nil
	})

	return mq.ReplyOK(coormq.RespGetUserStorageDetails(ret))
}

func (svc *Service) GetStorageByName(msg *coormq.GetStorageByName) (*coormq.GetStorageByNameResp, *mq.CodeMessage) {
	stg, err := svc.db2.Storage().GetUserStorageByName(svc.db2.DefCtx(), msg.UserID, msg.Name)
	if err != nil {
		logger.Warnf("getting user storage by name: %s", err.Error())

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, mq.Failed(errorcode.DataNotFound, "storage not found")
		}

		return nil, mq.Failed(errorcode.OperationFailed, "get user storage failed")
	}

	return mq.ReplyOK(coormq.RespGetStorageByNameResp(stg))
}

func (svc *Service) StoragePackageLoaded(msg *coormq.StoragePackageLoaded) (*coormq.StoragePackageLoadedResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		// TODO 权限检查
		exists, err := svc.db2.Object().BatchTestObjectID(tx, msg.PinnedObjects)
		if err != nil {
			return fmt.Errorf("testing object id: %w", err)
		}

		pinned := make([]cdssdk.PinnedObject, 0, len(msg.PinnedObjects))
		for _, obj := range msg.PinnedObjects {
			if exists[obj] {
				pinned = append(pinned, cdssdk.PinnedObject{
					StorageID:  msg.StorageID,
					ObjectID:   obj,
					CreateTime: time.Now(),
				})
			}
		}

		err = svc.db2.PinnedObject().BatchTryCreate(tx, pinned)
		if err != nil {
			return fmt.Errorf("batch creating pinned object: %w", err)
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

	return mq.ReplyOK(coormq.RespStoragePackageLoaded())
}
