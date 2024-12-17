package db

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	datamapmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/datamap"
	"gitlink.org.cn/cloudream/storage/datamap/internal/models"
)

type HubDB struct {
	*DB
}

func (db *DB) Hub() *HubDB {
	return &HubDB{DB: db}
}

func (*HubDB) GetAllHubs(ctx SQLContext) ([]models.Hub, error) {
	var ret []models.Hub

	err := ctx.Table("Hub").Find(&ret).Error
	return ret, err
}

func (*HubDB) GetHubStat(msg *datamapmq.GetHubStat) (*datamapmq.GetHubStatResp, *mq.CodeMessage) {
	//todo 数据库操作

	return mq.ReplyOK(datamapmq.NewGetHubStatResp(stgmod.HubStat{}))
}
