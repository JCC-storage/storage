package db

import "gitlink.org.cn/cloudream/storage/datamap/internal/models"

type StorageDB struct {
	*DB
}

func (db *DB) Storage() *StorageDB {
	return &StorageDB{DB: db}
}

// GetAllStorage 查询所有Storage列表
func (*HubDB) GetAllStorage(ctx SQLContext) ([]models.Storage, error) {
	var ret []models.Storage

	err := ctx.Table("storage").Find(&ret).Error
	return ret, err
}

// GetStorage 根据输入的StorageId查询Storage
func (*HubDB) GetStorage(ctx SQLContext, storageId int64) (models.Storage, error) {
	var ret models.Storage
	err := ctx.Table("storage").Where("StorageID = ?", storageId).Find(&ret).Error
	return ret, err
}

// DeleteStorage 根据输入的StorageId删除Storage
func (*HubDB) DeleteStorage(ctx SQLContext, storageId int64) error {
	return ctx.Table("storage").Where("StorageID = ?", storageId).Delete(&models.Storage{}).Error
}

// UpdateStorage 根据输入的Storage信息更新Storage
func (*HubDB) UpdateStorage(ctx SQLContext, storage models.Storage) error {
	return ctx.Table("storage").Where("StorageID = ?", storage.StorageID).Updates(&storage).Error
}
