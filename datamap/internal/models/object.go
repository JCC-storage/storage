package models

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/gorm"
	"log"
	"time"
)

type Object struct {
	ObjectID       cdssdk.ObjectID  `gorm:"column:ObjectID; primaryKey; type:bigint; autoIncrement" json:"objectID"`
	PackageID      cdssdk.PackageID `gorm:"column:PackageID; type:bigint; not null" json:"packageID"`
	Path           string           `gorm:"column:Path; type:varchar(1024); not null" json:"path"`
	Size           int64            `gorm:"column:Size; type:bigint; not null" json:"size"`
	FileHash       string           `gorm:"column:FileHash; type:varchar(255); not null" json:"fileHash"`
	Status         Status           `gorm:"column:Status; type:tinyint; not null" json:"status"`
	FaultTolerance float64          `gorm:"column:faultTolerance; type:float; not null" json:"faultTolerance"`
	Redundancy     float64          `gorm:"column:redundancy; type:float; not null" json:"redundancy"`
	AvgAccessCost  float64          `gorm:"column:avgAccessCost; type:float; not null" json:"avgAccessCost"`
	Timestamp      time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}

// BlockTransferRepository 块传输记录的 Repository
type BlockTransferRepository struct {
	repo *GormRepository
}

// NewBlockTransferRepository 创建 BlockTransferRepository 实例
func NewBlockTransferRepository(db *gorm.DB) *BlockTransferRepository {
	return &BlockTransferRepository{repo: NewGormRepository(db)}
}

// CreateBlockTransfer 创建块传输记录
func (r *BlockTransferRepository) CreateBlockTransfer(transfer *Object) error {
	return r.repo.Create(transfer)
}

// GetAllBlockTransfers 获取所有块传输记录
func (r *BlockTransferRepository) GetAllBlockTransfers() ([]Object, error) {
	var transfers []Object
	err := r.repo.GetAll(&transfers)
	if err != nil {
		return nil, err
	}
	return transfers, nil
}

// ProcessBlockTransInfo 处理 BlockTransInfo 数据
func ProcessBlockTransInfo(data stgmod.Object) {
	repo := NewBlockTransferRepository(db)
	//TODO 数据待调整
	transfer := &Object{
		ObjectID:       0,
		PackageID:      0,
		Path:           data.Path,
		Size:           0,
		FileHash:       "",
		Status:         0,
		FaultTolerance: 0,
		Redundancy:     0,
		AvgAccessCost:  0,
		Timestamp:      time.Time{},
	}
	err := repo.CreateBlockTransfer(transfer)
	if err != nil {
		log.Printf("Failed to create block transfer record: %v", err)
	}

}
