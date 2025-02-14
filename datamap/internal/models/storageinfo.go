package models

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gorm.io/gorm"
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

	switch body := event.Body.(type) {
	case *stgmod.BodyNewStorage:
		storage := &Storage{
			StorageID:   body.Info.StorageID,
			StorageName: body.Info.Name,
			HubID:       body.Info.MasterHub,
			Timestamp:   time.Now(),
		}
		err := repo.CreateStorage(storage)
		if err != nil {
			return
		}

	case *stgmod.BodyStorageUpdated:
		storage := &Storage{
			StorageID:   body.Info.StorageID,
			StorageName: body.Info.Name,
			HubID:       body.Info.MasterHub,
			Timestamp:   time.Now(),
		}
		err := repo.UpdateStorage(storage)
		if err != nil {
			return
		}
	case *stgmod.BodyStorageDeleted:
		storage := &Storage{
			StorageID: body.StorageID,
		}
		err := repo.DeleteStorage(storage)
		if err != nil {
			return
		}
	}
}
