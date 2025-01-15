package models

import (
	"errors"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
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

// ProcessBlockDistribution mq推送各节点统计自身当前的总数据量时的处理逻辑

func ProcessBlockDistribution(data stgmod.BlockDistribution) {
	repoObject := NewObjectRepository(DB)
	repoBlock := NewBlockDistributionRepository(DB)
	repoStorage := NewStorageTransferCountRepository(DB)
	//更新object表中的状态
	object, err := repoObject.GetObjectByID(data.Body.Object.ObjectID)
	faultTolerance, _ := strconv.ParseFloat(data.Body.Object.FaultTolerance, 64)
	redundancy, _ := strconv.ParseFloat(data.Body.Object.Redundancy, 64)
	avgAccessCost, _ := strconv.ParseFloat(data.Body.Object.AvgAccessCost, 64)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err := repoObject.CreateObject(&Object{
			ObjectID:       cdssdk.ObjectID(data.Body.Object.ObjectID),
			PackageID:      cdssdk.PackageID(data.Body.Object.PackageID),
			Path:           data.Body.Object.Path,
			Size:           data.Body.Object.Size,
			FileHash:       data.Body.Object.FileHash,
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
	for _, blockDistribution := range data.Body.Object.BlockDistribution {
		blockIndex, _ := strconv.ParseInt(blockDistribution.Index, 10, 64)
		blockStorageID, _ := strconv.ParseInt(blockDistribution.StorageID, 10, 64)
		blockDist, err := repoBlock.GetBlockDistributionByIndex(data.Body.Object.ObjectID, blockIndex, blockStorageID)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err := repoBlock.CreateBlockDistribution(&BlockDistribution{
				BlockID:   blockDist.BlockID,
				ObjectID:  blockDist.ObjectID,
				Type:      blockDistribution.Type,
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
				Type:      blockDistribution.Type,
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
	for _, dataTransfer := range data.Body.Object.DataTransfers {
		sourceStorageID, _ := strconv.ParseInt(dataTransfer.SourceStorageID, 10, 64)
		targetStorageID, _ := strconv.ParseInt(dataTransfer.TargetStorageID, 10, 64)
		dataTransferCount, _ := strconv.ParseInt(dataTransfer.DataTransferCount, 10, 64)

		err := repoStorage.CreateStorageTransferCount(&StorageTransferCount{
			ObjectID:          data.Body.Object.ObjectID,
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
}
