package models

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/gorm"
	"strconv"
	"time"
)

type BlockDistribution struct {
	BlockID   int64            `gorm:"column:BlockID; primaryKey; type:bigint; autoIncrement" json:"blockID"`
	ObjectID  cdssdk.ObjectID  `gorm:"column:ObjectID; type:bigint; not null" json:"objectID"`
	Type      string           `gorm:"column:Type; type:varchar(1024); not null" json:"type"`
	Index     int64            `gorm:"column:Index; type:bigint; not null" json:"index"`
	StorageID cdssdk.StorageID `gorm:"column:StorageID; type:bigint; not null" json:"storageID"`
	Status    Status           `gorm:"column:Status; type:tinyint; not null" json:"status"`
	Timestamp time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
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

func (r *BlockDistributionRepository) GetAllBlocks() ([]BlockDistribution, error) {
	var blocks []BlockDistribution
	err := r.repo.GetAll(&blocks)
	if err != nil {
		return nil, err
	}
	return blocks, nil
}

func ProcessBlockDistributionInfo(data stgmod.BlockTransInfo) {
	repo := NewBlockDistributionRepository(db)

	for _, change := range data.Body.BlockChanges {
		objectID, _ := strconv.ParseInt(data.Body.ObjectID, 10, 64)
		index, _ := strconv.ParseInt(change.Index, 10, 64)
		targetStorageID, _ := strconv.ParseInt(change.TargetStorageID, 10, 64)
		block := &BlockDistribution{
			ObjectID:  cdssdk.ObjectID(objectID),
			Type:      change.Type,
			Index:     index,
			StorageID: cdssdk.StorageID(targetStorageID),
		}
		repo.CreateBlockDistribution(block)
	}
}
