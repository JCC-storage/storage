package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gorm.io/gorm/clause"
)

type HubConnectivityDB struct {
	*DB
}

func (db *DB) HubConnectivity() *HubConnectivityDB {
	return &HubConnectivityDB{DB: db}
}

func (db *HubConnectivityDB) BatchGetByFromHub(ctx SQLContext, fromHubIDs []cdssdk.HubID) ([]model.HubConnectivity, error) {
	if len(fromHubIDs) == 0 {
		return nil, nil
	}

	var ret []model.HubConnectivity

	err := ctx.Table("HubConnectivity").Where("FromHubID IN (?)", fromHubIDs).Find(&ret).Error
	return ret, err
}

func (db *HubConnectivityDB) BatchUpdateOrCreate(ctx SQLContext, cons []model.HubConnectivity) error {
	if len(cons) == 0 {
		return nil
	}

	// 使用 GORM 的批量插入或更新
	return ctx.Table("HubConnectivity").Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&cons).Error
}
