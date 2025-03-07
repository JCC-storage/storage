package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type UserBucketDB struct {
	*DB
}

func (db *DB) UserBucket() *UserBucketDB {
	return &UserBucketDB{DB: db}
}

func (*UserBucketDB) GetByUserID(ctx SQLContext, userID cdssdk.UserID) ([]model.UserBucket, error) {
	var userBuckets []model.UserBucket
	err := ctx.Table("UserBucket").Where("UserID = ?", userID).Find(&userBuckets).Error
	return userBuckets, err
}

func (*UserBucketDB) Create(ctx SQLContext, userID cdssdk.UserID, bucketID cdssdk.BucketID) error {
	userBucket := model.UserBucket{
		UserID:   userID,
		BucketID: bucketID,
	}
	return ctx.Table("UserBucket").Create(&userBucket).Error
}

func (*UserBucketDB) DeleteByBucketID(ctx SQLContext, bucketID cdssdk.BucketID) error {
	return ctx.Table("UserBucket").Where("BucketID = ?", bucketID).Delete(&model.UserBucket{}).Error
}

func (*UserBucketDB) DeleteByUserID(ctx SQLContext, userID cdssdk.UserID) error {
	return ctx.Table("UserBucket").Where("UserID = ?", userID).Delete(&model.UserBucket{}).Error
}
