package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type UserHubDB struct {
	*DB
}

func (db *DB) UserHub() *UserHubDB {
	return &UserHubDB{db}
}

func (*UserHubDB) GetByUserID(ctx SQLContext, userID cdssdk.UserID) ([]model.UserHub, error) {
	var userHubs []model.UserHub
	if err := ctx.Table("UserHub").Where("UserID = ?", userID).Find(&userHubs).Error; err != nil {
		return nil, err
	}

	return userHubs, nil
}

func (*UserHubDB) Create(ctx SQLContext, userID cdssdk.UserID, hubID cdssdk.HubID) error {
	return ctx.Table("UserHub").Create(&model.UserHub{
		UserID: userID,
		HubID:  hubID,
	}).Error
}

func (*UserHubDB) DeleteByUserID(ctx SQLContext, userID cdssdk.UserID) error {
	return ctx.Table("UserHub").Delete(&model.UserHub{}, "UserID = ?", userID).Error
}
