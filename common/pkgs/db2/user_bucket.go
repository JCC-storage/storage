package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type UserBucketDB struct {
	*DB
}

func (db *DB) UserBucket() *UserBucketDB {
	return &UserBucketDB{DB: db}
}

func (*UserBucketDB) Create(ctx SQLContext, userID int64, bucketID int64) error {
	userBucket := model.UserBucket{
		UserID:   cdssdk.UserID(userID),
		BucketID: cdssdk.BucketID(bucketID),
	}
	return ctx.Table("UserBucket").Create(&userBucket).Error
}
