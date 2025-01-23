package models

import (
	"errors"
	"fmt"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gorm.io/gorm"
	"log"
	"strconv"
	"time"
)

type StorageTransferCount struct {
	RelationshipID    int64     `gorm:"column:RelationshipID; primaryKey; type:bigint; autoIncrement" json:"relationshipID"`
	ObjectID          int64     `gorm:"column:ObjectID; type:bigint; not null" json:"objectID"`
	Status            int64     `gorm:"column:Status; type:bigint; not null" json:"status"`                       // 连线左侧的状态
	SourceStorageID   int64     `gorm:"column:SourceStorageID; type:bigint; not null" json:"sourceStorageID"`     // 源存储节点 ID
	TargetStorageID   int64     `gorm:"column:TargetStorageID; type:bigint; not null" json:"targetStorageID"`     // 目标存储节点 ID
	DataTransferCount int64     `gorm:"column:DataTransferCount; type:bigint; not null" json:"dataTransferCount"` // 数据传输量
	Timestamp         time.Time `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`               // 变化结束时间戳
}

func (StorageTransferCount) TableName() string {
	return "storagetransfercount"
}

type StorageTransferCountRepository struct {
	repo *GormRepository
}

func NewStorageTransferCountRepository(db *gorm.DB) *StorageTransferCountRepository {
	return &StorageTransferCountRepository{repo: NewGormRepository(db)}
}

func (r *StorageTransferCountRepository) CreateStorageTransferCount(storageTransferCount *StorageTransferCount) error {
	return r.repo.Create(storageTransferCount)
}

func (r *StorageTransferCountRepository) UpdateStorageTransferCount(storageTransferCount *StorageTransferCount) error {
	return r.repo.Update(storageTransferCount)
}

func (r *StorageTransferCountRepository) GetStorageTransferCountByID(id int) (*StorageTransferCount, error) {
	var storageTransferCount StorageTransferCount
	err := r.repo.GetByID(uint(id), &storageTransferCount)
	if err != nil {
		return nil, err
	}
	return &storageTransferCount, nil
}

func (r *StorageTransferCountRepository) GetStorageTransferCountByObjectID(objectID int64) ([]StorageTransferCount, error) {
	var storageTransferCounts []StorageTransferCount
	query := "SELECT * FROM storagetransfercount WHERE ObjectID = ?"
	err := r.repo.db.Raw(query, objectID).Scan(&storageTransferCounts).Error
	if err != nil {
		return nil, err
	}
	return storageTransferCounts, nil
}

func (r *StorageTransferCountRepository) GetAllStorageTransferCounts() ([]StorageTransferCount, error) {
	var storageTransferCounts []StorageTransferCount
	err := r.repo.GetAll(&storageTransferCounts)
	if err != nil {
		return nil, err
	}
	return storageTransferCounts, nil
}

type BlockTransferWatcher struct {
	Name string
}

func (w *BlockTransferWatcher) OnEvent(event sysevent.SysEvent) {

	repoDist := NewBlockDistributionRepository(DB)
	repoStorage := NewStorageRepository(DB)
	repoStorageTrans := NewStorageTransferCountRepository(DB)
	repoObject := NewObjectRepository(DB)

	if event.Category == "blockTransfer" {
		if blockTransfer, ok := event.Body.(*stgmod.BodyBlockTransfer); ok {

			for _, change := range blockTransfer.BlockChanges {

				objectID, _ := strconv.ParseInt(string(blockTransfer.ObjectID), 10, 64)
				object, _ := repoObject.GetObjectByID(objectID)
				index, _ := strconv.ParseInt(change.Index, 10, 64)
				sourceStorageID, _ := strconv.ParseInt(string(change.SourceStorageID), 10, 64)
				targetStorageID, _ := strconv.ParseInt(string(change.TargetStorageID), 10, 64)
				newDataCount, _ := strconv.ParseInt(change.DataTransferCount, 10, 64)

				switch change.Type {
				case "0": //拷贝
					//查询出存储在数据库中的BlockDistribution信息
					blockSource, errSource := repoDist.GetBlockDistributionByIndex(objectID, index, sourceStorageID)
					//没有记录就将source和target的信息都保存到库中
					if errors.Is(errSource, gorm.ErrRecordNotFound) {
						err := repoDist.CreateBlockDistribution(&BlockDistribution{
							ObjectID:  objectID,
							Type:      change.BlockType,
							Index:     index,
							StorageID: sourceStorageID,
							Status:    StatusNow,
							Timestamp: time.Now(),
						})
						if err != nil {
							log.Printf("Error create source blockdistribution: %v", err)
						}
					} else {
						//有数据则新增一条storageID为targetStorageID的记录,同时更新状态
						err := repoDist.CreateBlockDistribution(&BlockDistribution{
							ObjectID:  blockSource.ObjectID,
							Type:      change.BlockType,
							Index:     index,
							StorageID: targetStorageID,
							Status:    StatusNow,
							Timestamp: time.Now(),
						})
						if err != nil {
							log.Printf("Error update blockdistribution: %v", err)
						}
						//复制完成之后增加的dataCount要加到targetStorage的记录中
						storageOld, err := repoStorage.GetStorageByID(targetStorageID)
						if errors.Is(err, gorm.ErrRecordNotFound) {
							err = repoStorage.CreateStorage(&Storage{
								StorageID: cdssdk.StorageID(targetStorageID),
								DataCount: newDataCount,
								Timestamp: time.Now(),
							})
							if err != nil {
								log.Printf("Error increase datacount in targetstorage: %v", err)
							}
						} else {
							err = repoStorage.UpdateStorage(&Storage{
								StorageID: cdssdk.StorageID(targetStorageID),
								DataCount: storageOld.DataCount + newDataCount,
								Timestamp: time.Now(),
							})
							if err != nil {
								log.Printf("Error increase datacount in targetstorage: %v", err)
							}
						}

					}
					//新增记录到storageTransferCount表中
					err := repoStorageTrans.CreateStorageTransferCount(&StorageTransferCount{
						ObjectID:          objectID,
						Status:            int64(blockSource.Status),
						SourceStorageID:   sourceStorageID,
						TargetStorageID:   targetStorageID,
						DataTransferCount: newDataCount,
						Timestamp:         time.Now(),
					})
					if err != nil {
						log.Printf("Error create StorageTransferCount : %v", err)
					}
				case "1": //编解码
					//删除所有的sourceBlock
					for _, sourceBlock := range change.SourceBlocks {
						sourceBlockIndex, _ := strconv.ParseInt(sourceBlock.Index, 10, 64)
						err := repoDist.DeleteBlockDistribution(objectID, sourceBlockIndex, sourceStorageID)
						if err != nil {
							log.Printf("Error delete blockdistribution: %v", err)
						}
					}
					//插入所有的targetBlock
					for _, targetBlock := range change.TargetBlocks {
						storageID, _ := strconv.ParseInt(string(targetBlock.StorageID), 10, 64)
						err := repoDist.CreateBlockDistribution(&BlockDistribution{
							ObjectID: objectID,
							Type:     targetBlock.BlockType,
							Index:    index,
							//直接保存到目标中心
							StorageID: storageID,
							Status:    StatusNow,
							Timestamp: time.Now(),
						})
						if err != nil {
							log.Printf("Error create blockdistribution: %v", err)
						}
					}
					//新增记录到storageTransferCount表中
					err := repoStorageTrans.CreateStorageTransferCount(&StorageTransferCount{
						ObjectID:          objectID,
						Status:            int64(object.Status),
						SourceStorageID:   sourceStorageID,
						TargetStorageID:   targetStorageID,
						DataTransferCount: newDataCount,
						Timestamp:         time.Now(),
					})
					if err != nil {
						log.Printf("Error create StorageTransferCount : %v", err)
					}

				case "2": //删除
					for _, block := range change.Blocks {
						storageID, _ := strconv.ParseInt(string(block.StorageID), 10, 64)
						changeIndex, _ := strconv.ParseInt(block.Index, 10, 64)
						err := repoDist.DeleteBlockDistribution(objectID, changeIndex, storageID)
						if err != nil {
							log.Printf("Error delete blockdistribution: %v", err)
						}
					}

				case "3": //更新
					for _, blockUpdate := range change.Blocks {
						//查询出存储在数据库中的BlockDistribution信息
						blockIndex, _ := strconv.ParseInt(blockUpdate.Index, 10, 64)
						blockOld, err := repoDist.GetBlockDistributionByIndex(objectID, blockIndex, sourceStorageID)
						newStorageID, _ := strconv.ParseInt(string(blockUpdate.StorageID), 10, 64)
						err = repoDist.UpdateBlockDistribution(&BlockDistribution{
							BlockID:   blockOld.BlockID,
							ObjectID:  blockOld.ObjectID,
							Type:      blockUpdate.BlockType,
							Index:     blockIndex,
							StorageID: newStorageID,
							Status:    StatusNow,
							Timestamp: time.Now(),
						})
						if err != nil {
							log.Printf("Error delete blockdistribution: %v", err)
						}
					}

				default:
					break
				}
			}
		} else {
			fmt.Printf("Watcher %s: Unexpected Body type, expected *BodyStorageInfo, got %T\n", w.Name, event.Body)
		}
	} else {
		fmt.Printf("Watcher %s received an event with category %s\n", w.Name, event.Category)
	}
}
