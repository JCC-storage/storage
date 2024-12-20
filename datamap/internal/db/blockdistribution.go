package db

import "gitlink.org.cn/cloudream/storage/datamap/internal/models"

type BlockDistributionDB struct {
	*DB
}

func (db *DB) BlockDistribution() *BlockDistributionDB {
	return &BlockDistributionDB{DB: db}
}

// GetAllBlockDistribution 查询所有BlockDistribution列表
func (*HubDB) GetAllBlockDistribution(ctx SQLContext) ([]models.BlockDistribution, error) {
	var ret []models.BlockDistribution

	err := ctx.Table("blockdistribution").Find(&ret).Error
	return ret, err
}

// GetBlockDistribution 根据输入的BlockID查询BlockDistribution
func (*HubDB) GetBlockDistribution(ctx SQLContext, BlockID int64) (models.BlockDistribution, error) {
	var ret models.BlockDistribution
	err := ctx.Table("blockdistribution").Where("BlockID = ?", BlockID).Find(&ret).Error
	return ret, err
}

// CreateBlockDistribution 根据输入的BlockDistribution信息创建BlockDistribution记录
func (*HubDB) CreateBlockDistribution(ctx SQLContext, blockDistribution models.BlockDistribution) (*models.BlockDistribution, error) {
	err := ctx.Table("blockdistribution").Create(&blockDistribution).Error
	return &blockDistribution, err
}

// DeleteBlockDistribution 根据输入的BlockID删除BlockDistribution记录
func (*HubDB) DeleteBlockDistribution(ctx SQLContext, BlockID int64) error {
	return ctx.Table("blockdistribution").Where("BlockID = ?", BlockID).Delete(&models.BlockDistribution{}).Error
}

// UpdateBlockDistribution 根据输入的BlockDistribution信息更新BlockDistribution记录
func (*HubDB) UpdateBlockDistribution(ctx SQLContext, blockDistribution models.BlockDistribution) error {
	return ctx.Table("blockdistribution").Where("BlockID = ?", blockDistribution.BlockID).Updates(&blockDistribution).Error
}
