package db

import "gitlink.org.cn/cloudream/storage/datamap/internal/models"

type HubReqDB struct {
	*DB
}

func (db *DB) HubReq() *HubReqDB {
	return &HubReqDB{DB: db}
}

// GetHubRequest 获取所有hubrequest列表
func (*HubReqDB) GetHubRequest(ctx SQLContext) ([]models.HubRequest, error) {
	var ret []models.HubRequest
	err := ctx.Table("hubrequest").Find(&ret).Error
	return ret, err
}

// CreateHubRequest 根据输入的HubRequest信息创建HubRequest信息
func (*HubReqDB) CreateHubRequest(ctx SQLContext, hubRequest models.HubRequest) (*models.HubRequest, error) {
	err := ctx.Table("hubrequest").Create(&hubRequest).Error
	return &hubRequest, err
}
