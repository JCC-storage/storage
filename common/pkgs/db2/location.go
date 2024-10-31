package db2

import (
	"fmt"

	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type LocationDB struct {
	*DB
}

func (db *DB) Location() *LocationDB {
	return &LocationDB{DB: db}
}

func (*LocationDB) GetByID(ctx SQLContext, id int64) (model.Location, error) {
	var ret model.Location
	err := ctx.First(&ret, id).Error
	return ret, err
}

func (db *LocationDB) FindLocationByExternalIP(ctx SQLContext, ip string) (model.Location, error) {
	var locID int64
	err := ctx.Table("Node").Select("LocationID").Where("ExternalIP = ?", ip).Scan(&locID).Error
	if err != nil {
		return model.Location{}, fmt.Errorf("finding node by external ip: %w", err)
	}

	loc, err := db.GetByID(ctx, locID)
	if err != nil {
		return model.Location{}, fmt.Errorf("getting location by id: %w", err)
	}

	return loc, nil
}
