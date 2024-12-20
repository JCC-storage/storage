package mq

import (
	"fmt"

	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) CachePackageMoved(msg *coormq.CachePackageMoved) (*coormq.CachePackageMovedResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		_, err := svc.db2.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		_, err = svc.db2.Storage().GetByID(tx, msg.StorageID)
		if err != nil {
			return fmt.Errorf("getting storage by id: %w", err)
		}

		err = svc.db2.PinnedObject().CreateFromPackage(tx, msg.PackageID, msg.StorageID)
		if err != nil {
			return fmt.Errorf("creating pinned objects from package: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).WithField("HubID", msg.StorageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "create package pinned objects failed")
	}

	return mq.ReplyOK(coormq.NewCachePackageMovedResp())
}

func (svc *Service) CacheRemovePackage(msg *coormq.CacheRemovePackage) (*coormq.CacheRemovePackageResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		_, err := svc.db2.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		_, err = svc.db2.Storage().GetByID(tx, msg.StorageID)
		if err != nil {
			return fmt.Errorf("getting storage by id: %w", err)
		}

		err = svc.db2.PinnedObject().DeleteInPackageAtStorage(tx, msg.PackageID, msg.StorageID)
		if err != nil {
			return fmt.Errorf("delete pinned objects in package at storage: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).WithField("HubID", msg.StorageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "remove pinned package failed")
	}

	return mq.ReplyOK(coormq.RespCacheRemovePackage())
}
