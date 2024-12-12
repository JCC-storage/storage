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

func (*HubDB) GetAllHubs(ctx SQLContext) ([]models.Hub, error) {
	var ret []models.Hub

	err := ctx.Table("Hub").Find(&ret).Error
	return ret, err
}
