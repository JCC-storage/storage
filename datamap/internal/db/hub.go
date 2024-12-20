package db

import (
	"gitlink.org.cn/cloudream/storage/datamap/internal/models"
)

type HubDB struct {
	*DB
}

func (db *DB) Hub() *HubDB {
	return &HubDB{DB: db}
}

// GetHubs 获取所有hub列表
func (*HubDB) GetHubs(ctx SQLContext) ([]models.Hub, error) {
	var ret []models.Hub
	err := ctx.Table("hub").Find(&ret).Error
	return ret, err
}

// GetHub 根据hubId获取该Hub的信息
func (*HubDB) GetHub(ctx SQLContext, hubID int64) (*models.Hub, error) {
	var hub models.Hub
	err := ctx.Table("hub").Where("HubID = ?", hubID).Find(&hub).Error
	return &hub, err
}

// DeleteHub 根据hubId删除该Hub
func (*HubDB) DeleteHub(ctx SQLContext, hubID int64) error {
	err := ctx.Table("hub").Where("HubID = ?", hubID).Delete(&models.Hub{}).Error
	return err
}

// UpdateHub 根据输入hub信息更新Hub信息
func (*HubDB) UpdateHub(ctx SQLContext, hub models.Hub) (*models.Hub, error) {
	err := ctx.Table("hub").Where("HubID = ?", hub.HubID).Updates(&hub).Error
	return &hub, err
}

// CreateHub 根据输入hub信息创建Hub信息
func (*HubDB) CreateHub(ctx SQLContext, hub models.Hub) (*models.Hub, error) {
	err := ctx.Table("hub").Create(&hub).Error
	return &hub, err
}

// IsHubExist 根据hubId查询hub是否存在
func (*HubDB) IsHubExist(ctx SQLContext, hubID int64) (bool, error) {
	var count int64
	err := ctx.Table("hub").Where("HubID = ?", hubID).Count(&count).Error
	return count > 0, err
}
