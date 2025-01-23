package models

import (
	"errors"
	"fmt"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gorm.io/gorm"
	"log"
	"strconv"
	"time"
)

type BlockDistribution struct {
	BlockID   int64     `gorm:"column:BlockID; primaryKey; type:bigint; autoIncrement" json:"blockID"`
	ObjectID  int64     `gorm:"column:ObjectID; type:bigint; not null" json:"objectID"`
	Type      string    `gorm:"column:Type; type:varchar(1024); not null" json:"type"`
	Index     int64     `gorm:"column:Index; type:bigint; not null" json:"index"`
	StorageID int64     `gorm:"column:StorageID; type:bigint; not null" json:"storageID"`
	Status    int       `gorm:"column:Status; type:tinyint; not null" json:"status"`
	Timestamp time.Time `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}

func (BlockDistribution) TableName() string {
	return "blockdistribution"
}

type BlockDistributionRepository struct {
	repo *GormRepository
}

func NewBlockDistributionRepository(db *gorm.DB) *BlockDistributionRepository {
	return &BlockDistributionRepository{repo: NewGormRepository(db)}
}

func (r *BlockDistributionRepository) CreateBlockDistribution(block *BlockDistribution) error {
	return r.repo.Create(block)
}

func (r *BlockDistributionRepository) UpdateBlockDistribution(block *BlockDistribution) error {
	return r.repo.Update(block)
}

func (r *BlockDistributionRepository) GetAllBlocks() ([]BlockDistribution, error) {
	var blocks []BlockDistribution
	err := r.repo.GetAll(&blocks)
	if err != nil {
		return nil, err
	}
	return blocks, nil
}

func (r *BlockDistributionRepository) GetBlockDistributionByObjectID(objectID int64) ([]BlockDistribution, error) {
	var blocks []BlockDistribution
	query := "SELECT * FROM blockdistribution WHERE ObjectID = ?"
	err := r.repo.db.Raw(query, objectID).Scan(&blocks).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return []BlockDistribution{}, errors.New("block not found")
	}
	return blocks, nil
}

func (r *BlockDistributionRepository) GetStorageIDsByObjectID(objectID int64) ([]int64, error) {
	var storageIDs []int64
	query := "SELECT distinct storageID FROM blockdistribution WHERE ObjectID = ?"
	// 通过 ObjectID 查询
	err := r.repo.db.Raw(query, objectID).Scan(&storageIDs).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return []int64{}, errors.New("block not found")
	}
	return storageIDs, nil
}

func (r *BlockDistributionRepository) GetBlockDistributionByIndex(objectID int64, index int64, storageID int64) (BlockDistribution, error) {
	var block BlockDistribution
	query := "SELECT * FROM blockdistribution WHERE ObjectID = ? AND `Index` = ? AND StorageID = ?"
	// 通过 ObjectID 和 Index 联合查询
	err := r.repo.db.Exec(query, objectID, index, storageID).First(&block).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return BlockDistribution{}, errors.New("block not found")
	}
	return block, nil
}

// DeleteBlockDistribution 删除 BlockDistribution 记录 (根据 ObjectID 和 Index)

func (r *BlockDistributionRepository) DeleteBlockDistribution(objectID int64, index int64, storageID int64) error {
	query := "DELETE FROM blockdistribution WHERE ObjectID = ? AND `Index` = ? AND StorageID = ?"

	return r.repo.db.Exec(query, objectID, index, storageID).Error
}

type BlockDistributionWatcher struct {
	Name string
}

func (w *BlockDistributionWatcher) OnEvent(event sysevent.SysEvent) {
	repoObject := NewObjectRepository(DB)
	repoBlock := NewBlockDistributionRepository(DB)
	repoStorage := NewStorageTransferCountRepository(DB)

	if event.Category == "blockDistribution" {
		if blockDistribution, ok := event.Body.(*stgmod.BodyBlockDistribution); ok {

			//更新object表中的状态

			object, err := repoObject.GetObjectByID(int64(blockDistribution.Object.ObjectID))
			faultTolerance, _ := strconv.ParseFloat(blockDistribution.Object.FaultTolerance, 64)
			redundancy, _ := strconv.ParseFloat(blockDistribution.Object.Redundancy, 64)
			avgAccessCost, _ := strconv.ParseFloat(blockDistribution.Object.AvgAccessCost, 64)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				err := repoObject.CreateObject(&Object{
					ObjectID:       blockDistribution.Object.ObjectID,
					PackageID:      blockDistribution.Object.PackageID,
					Path:           blockDistribution.Object.Path,
					Size:           blockDistribution.Object.Size,
					FileHash:       blockDistribution.Object.FileHash,
					Status:         StatusYesterdayAfter,
					FaultTolerance: faultTolerance,
					Redundancy:     redundancy,
					AvgAccessCost:  avgAccessCost,
					Timestamp:      time.Now(),
				})
				if err != nil {
					log.Printf("Error create object: %v", err)
				}
			} else {
				object.Status = StatusYesterdayAfter
				err = repoObject.UpdateObject(object)
				if err != nil {
					log.Printf("Error update object: %v", err)
				}
			}

			//更新block表中的状态
			for _, blockDist := range blockDistribution.Object.BlockDistribution {
				blockIndex, _ := strconv.ParseInt(blockDist.Index, 10, 64)
				blockStorageID, _ := strconv.ParseInt(blockDist.StorageID, 10, 64)
				blockDist, err := repoBlock.GetBlockDistributionByIndex(int64(blockDistribution.Object.ObjectID), blockIndex, blockStorageID)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					err := repoBlock.CreateBlockDistribution(&BlockDistribution{
						BlockID:   blockDist.BlockID,
						ObjectID:  blockDist.ObjectID,
						Type:      blockDist.Type,
						Index:     blockIndex,
						StorageID: blockStorageID,
						Status:    StatusYesterdayAfter,
						Timestamp: time.Now(),
					})
					if err != nil {
						log.Printf("Error create BlockDistribution: %v", err)
					}
				} else {
					err := repoBlock.UpdateBlockDistribution(&BlockDistribution{
						BlockID:   blockDist.BlockID,
						ObjectID:  blockDist.ObjectID,
						Type:      blockDist.Type,
						Index:     blockIndex,
						StorageID: blockStorageID,
						Status:    StatusYesterdayAfter,
						Timestamp: time.Now(),
					})
					if err != nil {
						log.Printf("Error update BlockDistribution: %v", err)
					}
				}
			}
			//在storageTransferCount表中添加记录
			for _, dataTransfer := range blockDistribution.Object.DataTransfers {
				sourceStorageID, _ := strconv.ParseInt(string(dataTransfer.SourceStorageID), 10, 64)
				targetStorageID, _ := strconv.ParseInt(string(dataTransfer.TargetStorageID), 10, 64)
				dataTransferCount, _ := strconv.ParseInt(dataTransfer.DataTransferCount, 10, 64)

				err := repoStorage.CreateStorageTransferCount(&StorageTransferCount{
					ObjectID:          int64(blockDistribution.Object.ObjectID),
					Status:            StatusTodayBeforeYesterday,
					SourceStorageID:   sourceStorageID,
					TargetStorageID:   targetStorageID,
					DataTransferCount: dataTransferCount,
					Timestamp:         time.Now(),
				})
				if err != nil {
					log.Printf("Error create StorageTransferCount : %v", err)
				}
			}
		} else {
			fmt.Printf("Watcher %s: Unexpected Body type, expected *BodyStorageInfo, got %T\n", w.Name, event.Body)
		}
	} else {
		fmt.Printf("Watcher %s received an event with category %s\n", w.Name, event.Category)
	}
}
