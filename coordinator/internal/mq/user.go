package mq

import (
	"errors"
	"fmt"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gorm.io/gorm"
)

func (svc *Service) CreateUser(msg *coormq.CreateUser) (*coormq.CreateUserResp, *mq.CodeMessage) {
	var user cdssdk.User
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		var err error

		user, err = svc.db2.User().Create(tx, msg.Name)
		if err != nil {
			return fmt.Errorf("creating user: %w", err)
		}

		// TODO 目前新建用户的权限与ID 1的相同
		hubs, err := svc.db2.UserHub().GetByUserID(tx, 1)
		if err != nil {
			return fmt.Errorf("getting user hubs: %w", err)
		}

		stgs, err := svc.db2.UserStorage().GetByUserID(tx, 1)
		if err != nil {
			return fmt.Errorf("getting user storages: %w", err)
		}

		for _, hub := range hubs {
			err := svc.db2.UserHub().Create(tx, user.UserID, hub.HubID)
			if err != nil {
				return fmt.Errorf("creating user hub: %w", err)
			}
		}

		for _, stg := range stgs {
			err := svc.db2.UserStorage().Create(tx, user.UserID, stg.StorageID)
			if err != nil {
				return fmt.Errorf("creating user storage: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		logger.WithField("Name", msg.Name).
			Warn(err.Error())

		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return nil, mq.Failed(errorcode.DataExists, "user name already exists")
		}

		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(coormq.RespCreateUser(user))
}

func (svc *Service) DeleteUser(msg *coormq.DeleteUser) (*coormq.DeleteUserResp, *mq.CodeMessage) {
	// TODO 目前不能删除ID 1的用户
	if msg.UserID == 1 {
		return nil, mq.Failed(errorcode.OperationFailed, "cannot delete the default user")
	}

	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		err := svc.db2.User().Delete(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("deleting user: %w", err)
		}

		err = svc.db2.UserHub().DeleteByUserID(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("deleting user hubs: %w", err)
		}

		err = svc.db2.UserStorage().DeleteByUserID(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("deleting user storages: %w", err)
		}

		bkts, err := svc.db2.UserBucket().GetByUserID(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("getting user buckets: %w", err)
		}

		for _, bkt := range bkts {
			pkgs, err := svc.db2.Package().GetBucketPackages(tx, bkt.BucketID)
			if err != nil {
				return fmt.Errorf("getting bucket packages: %w", err)
			}

			for _, pkg := range pkgs {
				err := svc.db2.Package().DeleteComplete(tx, pkg.PackageID)
				if err != nil {
					return fmt.Errorf("deleting package %v: %w", pkg.PackageID, err)
				}
			}

			err = svc.db2.Bucket().Delete(tx, bkt.BucketID)
			if err != nil {
				return fmt.Errorf("deleting bucket: %w", err)
			}
		}

		err = svc.db2.UserBucket().DeleteByUserID(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("deleting user buckets: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(coormq.RespDeleteUser())
}
