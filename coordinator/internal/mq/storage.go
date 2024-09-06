package mq

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetStorage(msg *coormq.GetStorage) (*coormq.GetStorageResp, *mq.CodeMessage) {
	stg, err := svc.db.Storage().GetUserStorage(svc.db.SQLCtx(), msg.UserID, msg.StorageID)
	if err != nil {
		logger.Warnf("getting user storage: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get user storage failed")
	}

	return mq.ReplyOK(coormq.RespGetStorage(stg))
}

func (svc *Service) GetStorageByName(msg *coormq.GetStorageByName) (*coormq.GetStorageByNameResp, *mq.CodeMessage) {
	stg, err := svc.db.Storage().GetUserStorageByName(svc.db.SQLCtx(), msg.UserID, msg.Name)
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
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		// 可以不用检查用户是否存在
		if ok, _ := svc.db.Package().IsAvailable(tx, msg.UserID, msg.PackageID); !ok {
			return fmt.Errorf("package is not available to user")
		}

		if ok, _ := svc.db.Storage().IsAvailable(tx, msg.UserID, msg.StorageID); !ok {
			return fmt.Errorf("storage is not available to user")
		}

		err := svc.db.StoragePackage().CreateOrUpdate(tx, msg.StorageID, msg.PackageID, msg.UserID)
		if err != nil {
			return fmt.Errorf("creating storage package: %w", err)
		}

		stg, err := svc.db.Storage().GetByID(tx, msg.StorageID)
		if err != nil {
			return fmt.Errorf("getting storage: %w", err)
		}

		err = svc.db.PinnedObject().CreateFromPackage(tx, msg.PackageID, stg.NodeID)
		if err != nil {
			return fmt.Errorf("creating pinned object from package: %w", err)
		}

		if len(msg.PinnedBlocks) > 0 {
			err = svc.db.ObjectBlock().BatchCreate(tx, msg.PinnedBlocks)
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
