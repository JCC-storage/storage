package db

import "gitlink.org.cn/cloudream/storage/datamap/internal/models"

type StorageTransferCountDB struct {
	*DB
}

func (db *DB) StorageTransferCount() *StorageTransferCountDB {
	return &StorageTransferCountDB{DB: db}
}

// GetAllStorageTransferCount 查询所有Storage列表
func (*HubDB) GetAllStorageTransferCount(ctx SQLContext) ([]models.StorageTransferCount, error) {
	var ret []models.StorageTransferCount

	err := ctx.Table("storagetransfercount").Find(&ret).Error
	return ret, err
}

// GetStorageTransferCount 根据输入的RelationshipID查询StorageTransferCount
func (*HubDB) GetStorageTransferCount(ctx SQLContext, RelationshipID int64) (models.Storage, error) {
	var ret models.Storage
	err := ctx.Table("storagetransfercount").Where("RelationshipID = ?", RelationshipID).Find(&ret).Error
	return ret, err
}

// DeleteStorageTransferCount 根据输入的RelationshipID删除StorageTransferCount
func (*HubDB) DeleteStorageTransferCount(ctx SQLContext, RelationshipID int64) error {
	return ctx.Table("storagetransfercount").Where("RelationshipID = ?", RelationshipID).Delete(&models.Storage{}).Error
}

// UpdateStorageTransferCount 根据输入的StorageTransferCount信息更新StorageTransferCount
func (*HubDB) UpdateStorageTransferCount(ctx SQLContext, storageTransferCount models.StorageTransferCount) error {
	return ctx.Table("storagetransfercount").Where("RelationshipID = ?", storageTransferCount.RelationshipID).Updates(&storageTransferCount).Error
}
