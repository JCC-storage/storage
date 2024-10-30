package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type ShardStorageDB struct {
	*DB
}

func (db *DB) ShardStorage() *ShardStorageDB {
	return &ShardStorageDB{DB: db}
}

func (*ShardStorageDB) GetByStorageID(ctx SQLContext, stgID cdssdk.StorageID) (cdssdk.ShardStorage, error) {
	var ret cdssdk.ShardStorage
	err := ctx.Table("ShardStorage").First(&ret, stgID).Error
	return ret, err
}

func (*ShardStorageDB) BatchGetByStorageIDs(ctx SQLContext, stgIDs []cdssdk.StorageID) ([]cdssdk.ShardStorage, error) {
	var ret []cdssdk.ShardStorage
	err := ctx.Table("ShardStorage").Find(&ret, "StorageID IN (?)", stgIDs).Error
	return ret, err
}
