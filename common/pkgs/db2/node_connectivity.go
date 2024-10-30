package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gorm.io/gorm/clause"
)

type NodeConnectivityDB struct {
	*DB
}

func (db *DB) NodeConnectivity() *NodeConnectivityDB {
	return &NodeConnectivityDB{DB: db}
}

func (db *NodeConnectivityDB) BatchGetByFromNode(ctx SQLContext, fromNodeIDs []cdssdk.NodeID) ([]model.NodeConnectivity, error) {
	if len(fromNodeIDs) == 0 {
		return nil, nil
	}

	var ret []model.NodeConnectivity

	err := ctx.Table("NodeConnectivity").Where("FromNodeID IN (?)", fromNodeIDs).Find(&ret).Error
	return ret, err
}

func (db *NodeConnectivityDB) BatchUpdateOrCreate(ctx SQLContext, cons []model.NodeConnectivity) error {
	if len(cons) == 0 {
		return nil
	}

	// 使用 GORM 的批量插入或更新
	return ctx.Table("NodeConnectivity").Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&cons).Error
}
