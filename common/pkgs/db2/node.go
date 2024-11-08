package db2

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type HubDB struct {
	*DB
}

func (db *DB) Hub() *HubDB {
	return &HubDB{DB: db}
}

func (*HubDB) GetAllHubs(ctx SQLContext) ([]cdssdk.Hub, error) {
	var ret []cdssdk.Hub

	err := ctx.Table("Hub").Find(&ret).Error
	return ret, err
}

func (*HubDB) GetByID(ctx SQLContext, hubID cdssdk.HubID) (cdssdk.Hub, error) {
	var ret cdssdk.Hub
	err := ctx.Table("Hub").Where("HubID = ?", hubID).Find(&ret).Error

	return ret, err
}

func (*HubDB) BatchGetByID(ctx SQLContext, hubIDs []cdssdk.HubID) ([]cdssdk.Hub, error) {
	var ret []cdssdk.Hub
	err := ctx.Table("Hub").Where("HubID IN (?)", hubIDs).Find(&ret).Error

	return ret, err
}

// GetUserHubs 根据用户id查询可用hub
func (*HubDB) GetUserHubs(ctx SQLContext, userID cdssdk.UserID) ([]cdssdk.Hub, error) {
	var hubs []cdssdk.Hub
	err := ctx.
		Table("Hub").
		Select("Hub.*").
		Joins("JOIN UserHub ON UserHub.HubID = Hub.HubID").
		Where("UserHub.UserID = ?", userID).
		Find(&hubs).Error
	return hubs, err
}

// UpdateState 更新状态，并且设置上次上报时间为现在
func (*HubDB) UpdateState(ctx SQLContext, hubID cdssdk.HubID, state string) error {
	err := ctx.
		Model(&cdssdk.Hub{}).
		Where("HubID = ?", hubID).
		Updates(map[string]interface{}{
			"State":          state,
			"LastReportTime": time.Now(),
		}).Error
	return err
}
