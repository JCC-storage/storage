package models

import (
	"errors"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/gorm"
	"log"
	"time"
)

type Storage struct {
	StorageID    cdssdk.StorageID `gorm:"column:StorageID; primaryKey; type:bigint; autoIncrement" json:"storageID"`
	StorageName  string           `gorm:"column:StorageName; type:varchar(1024); not null" json:"storageName"`
	HubID        cdssdk.HubID     `gorm:"column:HubID; type:bigint; not null" json:"hubID"`
	DataCount    int64            `gorm:"column:DataCount; type:bigint; not null" json:"dataCount"`
	NewDataCount int64            `gorm:"column:NewDataCount; type:bigint; not null" json:"newDataCount"`
	Timestamp    time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}

func (Storage) TableName() string { return "storage" }

type StorageRepository struct {
	repo *GormRepository
}

func NewStorageRepository(db *gorm.DB) *StorageRepository {
	return &StorageRepository{repo: NewGormRepository(db)}
}

func (r *StorageRepository) CreateStorage(storage *Storage) error {
	return r.repo.Create(storage)
}

func (r *StorageRepository) UpdateStorage(storage *Storage) error {
	return r.repo.Update(storage)
}

func (r *StorageRepository) GetStorageByID(id int64) (*Storage, error) {
	var storage Storage
	err := r.repo.GetByID(uint(id), &storage)
	if err != nil {
		return nil, err
	}
	return &storage, nil
}

func (r *StorageRepository) GetStorageByHubID(hubId int64) (*Storage, error) {
	var storage Storage
	query := "SELECT * FROM storage WHERE HubID = ?"
	err := r.repo.db.Raw(query, hubId).Scan(&storage).Error
	if err != nil {
		return nil, err
	}
	return &storage, nil
}

func (r *StorageRepository) GetAllStorages() ([]Storage, error) {
	var storages []Storage
	err := r.repo.GetAll(&storages)
	if err != nil {
		return nil, err
	}
	return storages, nil
}

//ProcessHubStat mq推送各节点统计自身当前的总数据量时的处理逻辑

func ProcessStorageInfo(data stgmod.StorageStats) {
	repo := NewStorageRepository(DB)

	storage, err := repo.GetStorageByID(data.Body.StorageID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 插入新记录
			newStorage := &Storage{
				StorageID:    cdssdk.StorageID(data.Body.StorageID),
				DataCount:    data.Body.DataCount,
				NewDataCount: 0,
			}
			repo.CreateStorage(newStorage)
		} else {
			log.Printf("Error querying storage: %v", err)
		}
	} else {
		// 更新记录
		newDataCount := data.Body.DataCount - storage.DataCount
		storage.DataCount = data.Body.DataCount
		storage.NewDataCount = newDataCount
		err := repo.UpdateStorage(storage)
		if err != nil {
			log.Printf("Error update storage: %v", err)
		}
	}
}
