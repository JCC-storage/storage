package mq

import (
	"errors"
	"fmt"

	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	"gorm.io/gorm"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetBucket(userID cdssdk.UserID, bucketID cdssdk.BucketID) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *Service) GetBucketByName(msg *coormq.GetBucketByName) (*coormq.GetBucketByNameResp, *mq.CodeMessage) {
	bucket, err := svc.db2.Bucket().GetUserBucketByName(svc.db2.DefCtx(), msg.UserID, msg.Name)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("Name", msg.Name).
			Warnf("getting bucket by name: %s", err.Error())

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, mq.Failed(errorcode.DataNotFound, "bucket not found")
		}

		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(coormq.RespGetBucketByName(bucket))
}

func (svc *Service) GetUserBuckets(msg *coormq.GetUserBuckets) (*coormq.GetUserBucketsResp, *mq.CodeMessage) {
	buckets, err := svc.db2.Bucket().GetUserBuckets(svc.db2.DefCtx(), msg.UserID)

	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("get user buckets failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(coormq.NewGetUserBucketsResp(buckets))
}

func (svc *Service) GetBucketPackages(msg *coormq.GetBucketPackages) (*coormq.GetBucketPackagesResp, *mq.CodeMessage) {
	packages, err := svc.db2.Package().GetBucketPackages(svc.db2.DefCtx(), msg.UserID, msg.BucketID)

	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warnf("get bucket packages failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get bucket packages failed")
	}

	return mq.ReplyOK(coormq.NewGetBucketPackagesResp(packages))
}

func (svc *Service) CreateBucket(msg *coormq.CreateBucket) (*coormq.CreateBucketResp, *mq.CodeMessage) {
	var bucket cdssdk.Bucket
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		_, err := svc.db2.User().GetByID(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("getting user by id: %w", err)
		}

		bucketID, err := svc.db2.Bucket().Create(tx, msg.UserID, msg.BucketName)
		if err != nil {
			return fmt.Errorf("creating bucket: %w", err)
		}

		bucket, err = svc.db2.Bucket().GetByID(tx, bucketID)
		if err != nil {
			return fmt.Errorf("getting bucket by id: %w", err)
		}
		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketName", msg.BucketName).
			Warn(err.Error())

		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return nil, mq.Failed(errorcode.DataExists, "bucket name already exists")
		}

		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(coormq.NewCreateBucketResp(bucket))
}

func (svc *Service) DeleteBucket(msg *coormq.DeleteBucket) (*coormq.DeleteBucketResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		isAvai, _ := svc.db2.Bucket().IsAvailable(tx, msg.BucketID, msg.UserID)
		if !isAvai {
			return fmt.Errorf("bucket is not avaiable to the user")
		}

		err := svc.db2.Bucket().Delete(tx, msg.BucketID)
		if err != nil {
			return fmt.Errorf("deleting bucket: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "delete bucket failed")
	}

	return mq.ReplyOK(coormq.NewDeleteBucketResp())
}
