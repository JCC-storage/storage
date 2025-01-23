package models

import (
	"fmt"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gorm.io/gorm"
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
func (r *StorageRepository) DeleteStorage(storage *Storage) error {
	return r.repo.Delete(storage, uint(storage.StorageID))
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

type StorageInfoWatcher struct {
	Name string
}

func (w *StorageInfoWatcher) OnEvent(event sysevent.SysEvent) {
	repo := NewStorageRepository(DB)

	if event.Category == "storageInfo" {
		if storageInfo, ok := event.Body.(*stgmod.BodyStorageInfo); ok {
			storage := &Storage{
				StorageID:   storageInfo.StorageID,
				StorageName: storageInfo.StorageInfo.Name,
				HubID:       storageInfo.StorageInfo.MasterHub,
				Timestamp:   time.Now(),
			}

			switch storageInfo.Type {
			case "add":
				err := repo.CreateStorage(storage)
				if err != nil {
					return
				}
			case "update":
				err := repo.UpdateStorage(storage)
				if err != nil {
					return
				}
			case "delete":
				err := repo.DeleteStorage(storage)
				if err != nil {
					return
				}
			default:
				return
			}
		} else {
			fmt.Printf("Watcher %s: Unexpected Body type, expected *BodyStorageInfo, got %T\n", w.Name, event.Body)
		}
	} else {
		fmt.Printf("Watcher %s received an event with category %s\n", w.Name, event.Category)
	}
}
