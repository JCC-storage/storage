package models

import (
	"errors"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/gorm"
	"log"
)

func ProcessStorageStats(data stgmod.StorageStats) {
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
