package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PackageAccessStatDB struct {
	*DB
}

func (db *DB) PackageAccessStat() *PackageAccessStatDB {
	return &PackageAccessStatDB{db}
}

func (*PackageAccessStatDB) Get(ctx SQLContext, pkgID cdssdk.PackageID, stgID cdssdk.StorageID) (stgmod.PackageAccessStat, error) {
	var ret stgmod.PackageAccessStat
	err := ctx.Table("PackageAccessStat").Where("PackageID = ? AND StorageID = ?", pkgID, stgID).First(&ret).Error
	return ret, err
}

func (*PackageAccessStatDB) GetByPackageID(ctx SQLContext, pkgID cdssdk.PackageID) ([]stgmod.PackageAccessStat, error) {
	var ret []stgmod.PackageAccessStat
	err := ctx.Table("PackageAccessStat").Where("PackageID = ?", pkgID).Find(&ret).Error
	return ret, err
}

func (*PackageAccessStatDB) BatchGetByPackageID(ctx SQLContext, pkgIDs []cdssdk.PackageID) ([]stgmod.PackageAccessStat, error) {
	if len(pkgIDs) == 0 {
		return nil, nil
	}

	var ret []stgmod.PackageAccessStat
	err := ctx.Table("PackageAccessStat").Where("PackageID IN (?)", pkgIDs).Find(&ret).Error
	return ret, err
}

func (*PackageAccessStatDB) BatchAddCounter(ctx SQLContext, entries []coormq.AddAccessStatEntry) error {
	if len(entries) == 0 {
		return nil
	}

	accs := make([]stgmod.PackageAccessStat, len(entries))
	for i, e := range entries {
		accs[i] = stgmod.PackageAccessStat{
			PackageID: e.PackageID,
			StorageID: e.StorageID,
			Counter:   e.Counter,
		}
	}

	return ctx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "PackageID"}, {Name: "StorageID"}},
		DoUpdates: clause.Assignments(map[string]any{
			"Counter": gorm.Expr("Counter + values(Counter)"),
		}),
	}).Table("PackageAccessStat").Create(&accs).Error
}

func (*PackageAccessStatDB) BatchUpdateAmount(ctx SQLContext, pkgIDs []cdssdk.PackageID, historyWeight float64) error {
	if len(pkgIDs) == 0 {
		return nil
	}

	sql := "UPDATE PackageAccessStat SET Amount = Amount * ? + Counter * (1 - ?), Counter = 0 WHERE PackageID IN (?)"
	return ctx.Exec(sql, historyWeight, historyWeight, pkgIDs).Error
}

func (*PackageAccessStatDB) UpdateAllAmount(ctx SQLContext, historyWeight float64) error {
	sql := "UPDATE PackageAccessStat SET Amount = Amount * ? + Counter * (1 - ?), Counter = 0"
	return ctx.Exec(sql, historyWeight, historyWeight).Error
}

func (*PackageAccessStatDB) DeleteByPackageID(ctx SQLContext, pkgID cdssdk.PackageID) error {
	return ctx.Table("PackageAccessStat").Where("PackageID = ?", pkgID).Delete(&stgmod.PackageAccessStat{}).Error
}
