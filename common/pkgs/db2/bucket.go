package db2

import (
	"errors"
	"fmt"

	"gorm.io/gorm"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type BucketDB struct {
	*DB
}

func (db *DB) Bucket() *BucketDB {
	return &BucketDB{DB: db}
}

func (db *BucketDB) GetByID(ctx SQLContext, bucketID cdssdk.BucketID) (cdssdk.Bucket, error) {
	var ret cdssdk.Bucket
	err := ctx.Table("Bucket").Where("BucketID = ?", bucketID).First(&ret).Error
	return ret, err
}

// GetIDByName 根据BucketName查询BucketID
func (db *BucketDB) GetIDByName(ctx SQLContext, bucketName string) (int64, error) {
	var result struct {
		BucketID   int64  `gorm:"column:BucketID"`
		BucketName string `gorm:"column:BucketName"`
	}

	err := ctx.Table("Bucket").Select("BucketID, BucketName").Where("BucketName = ?", bucketName).Scan(&result).Error
	if err != nil {
		return 0, err
	}

	return result.BucketID, nil
}

// IsAvailable 判断用户是否有指定Bucekt的权限
func (db *BucketDB) IsAvailable(ctx SQLContext, bucketID cdssdk.BucketID, userID cdssdk.UserID) (bool, error) {
	_, err := db.GetUserBucket(ctx, userID, bucketID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("find bucket failed, err: %w", err)
	}

	return true, nil
}

func (*BucketDB) GetUserBucket(ctx SQLContext, userID cdssdk.UserID, bucketID cdssdk.BucketID) (model.Bucket, error) {
	var ret model.Bucket
	err := ctx.Table("UserBucket").
		Select("Bucket.*").
		Joins("JOIN Bucket ON UserBucket.BucketID = Bucket.BucketID").
		Where("UserBucket.UserID = ? AND Bucket.BucketID = ?", userID, bucketID).
		First(&ret).Error
	return ret, err
}

func (*BucketDB) GetUserBucketByName(ctx SQLContext, userID cdssdk.UserID, bucketName string) (model.Bucket, error) {
	var ret model.Bucket
	err := ctx.Table("UserBucket").
		Select("Bucket.*").
		Joins("JOIN Bucket ON UserBucket.BucketID = Bucket.BucketID").
		Where("UserBucket.UserID = ? AND Bucket.Name = ?", userID, bucketName).
		First(&ret).Error
	return ret, err
}

func (*BucketDB) GetUserBuckets(ctx SQLContext, userID cdssdk.UserID) ([]model.Bucket, error) {
	var ret []model.Bucket
	err := ctx.Table("UserBucket").
		Select("Bucket.*").
		Joins("JOIN Bucket ON UserBucket.BucketID = Bucket.BucketID").
		Where("UserBucket.UserID = ?", userID).
		Find(&ret).Error
	return ret, err
}

func (db *BucketDB) Create(ctx SQLContext, userID cdssdk.UserID, bucketName string) (cdssdk.BucketID, error) {
	var bucketID int64
	err := ctx.Table("UserBucket").
		Select("Bucket.BucketID").
		Joins("JOIN Bucket ON UserBucket.BucketID = Bucket.BucketID").
		Where("UserBucket.UserID = ? AND Bucket.Name = ?", userID, bucketName).
		Scan(&bucketID).Error

	if err != nil {
		return 0, err
	}

	if bucketID > 0 {
		return 0, gorm.ErrDuplicatedKey
	}

	newBucket := cdssdk.Bucket{Name: bucketName, CreatorID: userID}
	if err := ctx.Table("Bucket").Create(&newBucket).Error; err != nil {
		return 0, fmt.Errorf("insert bucket failed, err: %w", err)
	}

	err = ctx.Table("UserBucket").Create(&model.UserBucket{UserID: userID, BucketID: newBucket.BucketID}).Error
	if err != nil {
		return 0, fmt.Errorf("insert user bucket: %w", err)
	}

	return newBucket.BucketID, nil
}

func (db *BucketDB) Delete(ctx SQLContext, bucketID cdssdk.BucketID) error {
	return ctx.Delete(&cdssdk.Bucket{}, "BucketID = ?", bucketID).Error
}
