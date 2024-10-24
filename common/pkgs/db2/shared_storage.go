package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type SharedStorageDB struct {
	*DB
}

func (db *DB) SharedStorage() *SharedStorageDB {
	return &SharedStorageDB{DB: db}
}

func (*SharedStorageDB) GetByStorageID(ctx SQLContext, stgID cdssdk.StorageID) (cdssdk.SharedStorage, error) {
	var ret cdssdk.SharedStorage
	err := ctx.Table("SharedStorage").First(&ret, stgID).Error
	return ret, err
}
