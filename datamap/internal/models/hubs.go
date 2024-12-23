package models

import (
	"errors"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/gorm"
	"log"
	"time"
)

var db *gorm.DB

func InitDB(gormDB *gorm.DB) {
	db = gormDB
}

type Hub struct {
	HubID   cdssdk.HubID          `gorm:"column:HubID; primaryKey; type:bigint; autoIncrement" json:"hubID"`
	Name    string                `gorm:"column:Name; type:varchar(255); not null" json:"name"`
	Address cdssdk.HubAddressInfo `gorm:"column:Address; type:json; serializer:union" json:"address"`
}

type HubStat struct {
	StorageID int `json:"storageID"`
	DataCount int `json:"dataCount"`
}

type Storage struct {
	StorageID    cdssdk.StorageID `gorm:"column:StorageID; primaryKey; type:bigint; autoIncrement" json:"storageID"`
	HubID        cdssdk.HubID     `gorm:"column:HubID; type:bigint; not null" json:"hubID"`
	DataCount    int64            `gorm:"column:DataCount; type:bigint; not null" json:"dataCount"`
	NewDataCount int64            `gorm:"column:NewDataCount; type:bigint; not null" json:"newDataCount"`
	Timestamp    time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}
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

func (r *StorageRepository) GetStorageByID(id int) (*Storage, error) {
	var storage Storage
	err := r.repo.GetByID(uint(id), &storage)
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

func ProcessHubStat(data stgmod.HubStat) {
	repo := NewStorageRepository(db)

	storage, err := repo.GetStorageByID(data.Body.StorageID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 插入新记录
			newStorage := &Storage{
				StorageID:    cdssdk.StorageID(data.Body.StorageID),
				DataCount:    int64(data.Body.DataCount),
				NewDataCount: 0,
			}
			repo.CreateStorage(newStorage)
		} else {
			log.Printf("Error querying storage: %v", err)
		}
	} else {
		// 更新记录
		newDataCount := int64(data.Body.DataCount) - storage.DataCount
		storage.DataCount = int64(data.Body.DataCount)
		storage.NewDataCount = newDataCount
		repo.UpdateStorage(storage)
	}
}
