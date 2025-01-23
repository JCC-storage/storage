package models

import (
	"errors"
	"fmt"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gorm.io/gorm"
	"log"
)

type StorageStatsWatcher struct {
	Name string
}

func (w *StorageStatsWatcher) OnEvent(event sysevent.SysEvent) {
	repo := NewStorageRepository(DB)

	if event.Category == "storageStats" {
		if storageStats, ok := event.Body.(*stgmod.BodyStorageStats); ok {

			storage, err := repo.GetStorageByID(int64(storageStats.StorageID))
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// 插入新记录
					newStorage := &Storage{
						StorageID:    storageStats.StorageID,
						DataCount:    storageStats.DataCount,
						NewDataCount: 0,
					}
					repo.CreateStorage(newStorage)
				} else {
					log.Printf("Error querying storage: %v", err)
				}
			} else {
				// 更新记录
				newDataCount := storageStats.DataCount - storage.DataCount
				storage.DataCount = storageStats.DataCount
				storage.NewDataCount = newDataCount
				err := repo.UpdateStorage(storage)
				if err != nil {
					log.Printf("Error update storage: %v", err)
				}
			}
		} else {
			fmt.Printf("Watcher %s: Unexpected Body type, expected *BodyStorageInfo, got %T\n", w.Name, event.Body)
		}
	} else {
		fmt.Printf("Watcher %s received an event with category %s\n", w.Name, event.Category)
	}
}
