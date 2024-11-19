package db2

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gorm.io/gorm"
)

type StorageDB struct {
	*DB
}

func (db *DB) Storage() *StorageDB {
	return &StorageDB{DB: db}
}

func (db *StorageDB) GetByID(ctx SQLContext, stgID cdssdk.StorageID) (model.Storage, error) {
	var stg model.Storage
	err := ctx.Table("Storage").First(&stg, stgID).Error
	return stg, err
}

func (StorageDB) GetAllIDs(ctx SQLContext) ([]cdssdk.StorageID, error) {
	var stgs []cdssdk.StorageID
	err := ctx.Table("Storage").Select("StorageID").Find(&stgs).Error
	return stgs, err
}

func (db *StorageDB) BatchGetByID(ctx SQLContext, stgIDs []cdssdk.StorageID) ([]model.Storage, error) {
	var stgs []model.Storage
	err := ctx.Table("Storage").Find(&stgs, "StorageID IN (?)", stgIDs).Error
	return stgs, err
}

func (db *StorageDB) GetUserStorages(ctx SQLContext, userID cdssdk.UserID) ([]model.Storage, error) {
	var stgs []model.Storage
	err := ctx.Table("Storage").Select("Storage.*").
		Joins("inner join UserStorage on Storage.StorageID = UserStorage.StorageID").
		Where("UserID = ?", userID).Find(&stgs).Error
	return stgs, err
}

func (db *StorageDB) BatchGetAllStorageIDs(ctx SQLContext, start int, count int) ([]cdssdk.StorageID, error) {
	var ret []cdssdk.StorageID
	err := ctx.Table("Storage").Select("StorageID").Find(&ret).Limit(count).Offset(start).Error
	return ret, err
}

func (db *StorageDB) IsAvailable(ctx SQLContext, userID cdssdk.UserID, storageID cdssdk.StorageID) (bool, error) {
	rows, err := ctx.Table("Storage").Select("Storage.StorageID").
		Joins("inner join UserStorage on Storage.StorageID = UserStorage.StorageID").
		Where("UserID = ? and Storage.StorageID = ?", userID, storageID).Rows()
	if err != nil {
		return false, fmt.Errorf("execute sql: %w", err)
	}
	defer rows.Close()

	return rows.Next(), nil
}

func (db *StorageDB) GetUserStorage(ctx SQLContext, userID cdssdk.UserID, storageID cdssdk.StorageID) (model.Storage, error) {
	var stg model.Storage
	err := ctx.Table("Storage").Select("Storage.*").
		Joins("inner join UserStorage on Storage.StorageID = UserStorage.StorageID").
		Where("UserID = ? and Storage.StorageID = ?", userID, storageID).First(&stg).Error

	return stg, err
}

func (db *StorageDB) GetUserStorageByName(ctx SQLContext, userID cdssdk.UserID, name string) (model.Storage, error) {
	var stg model.Storage
	err := ctx.Table("Storage").Select("Storage.*").
		Joins("inner join UserStorage on Storage.StorageID = UserStorage.StorageID").
		Where("UserID = ? and Name = ?", userID, name).First(&stg).Error

	return stg, err
}

func (db *StorageDB) GetHubStorages(ctx SQLContext, hubID cdssdk.HubID) ([]model.Storage, error) {
	var stgs []model.Storage
	err := ctx.Table("Storage").Select("Storage.*").Find(&stgs, "MasterHub = ?", hubID).Error
	return stgs, err
}

func (db *StorageDB) FillDetails(ctx SQLContext, details []stgmod.StorageDetail) error {
	stgsMp := make(map[cdssdk.StorageID]*stgmod.StorageDetail)
	var masterHubIDs []cdssdk.HubID
	for i := range details {
		stgsMp[details[i].Storage.StorageID] = &details[i]
		masterHubIDs = append(masterHubIDs, details[i].Storage.MasterHub)
	}

	// 获取监护Hub信息
	masterHubs, err := db.Hub().BatchGetByID(ctx, masterHubIDs)
	if err != nil && err != gorm.ErrRecordNotFound {
		return fmt.Errorf("getting master hub: %w", err)
	}
	masterHubMap := make(map[cdssdk.HubID]cdssdk.Hub)
	for _, hub := range masterHubs {
		masterHubMap[hub.HubID] = hub
	}
	for _, stg := range stgsMp {
		if stg.Storage.MasterHub != 0 {
			hub, ok := masterHubMap[stg.Storage.MasterHub]
			if !ok {
				logger.Warnf("master hub %v of storage %v not found, this storage will not be add to result", stg.Storage.MasterHub, stg.Storage)
				delete(stgsMp, stg.Storage.StorageID)
				continue
			}

			stg.MasterHub = &hub
		}
	}

	return nil
}
