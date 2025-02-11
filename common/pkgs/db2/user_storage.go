package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type UserStorageDB struct {
	*DB
}

func (db *DB) UserStorage() *UserStorageDB {
	return &UserStorageDB{db}
}

func (*UserStorageDB) GetByUserID(ctx SQLContext, userID cdssdk.UserID) ([]model.UserStorage, error) {
	var userStgs []model.UserStorage
	if err := ctx.Table("UserStorage").Where("UserID = ?", userID).Find(&userStgs).Error; err != nil {
		return nil, err
	}

	return userStgs, nil
}

func (*UserStorageDB) Create(ctx SQLContext, userID cdssdk.UserID, stgID cdssdk.StorageID) error {
	return ctx.Table("UserStorage").Create(&model.UserStorage{
		UserID:    userID,
		StorageID: stgID,
	}).Error
}

func (*UserStorageDB) DeleteByUserID(ctx SQLContext, userID cdssdk.UserID) error {
	return ctx.Table("UserStorage").Delete(&model.UserStorage{}, "UserID = ?", userID).Error
}
