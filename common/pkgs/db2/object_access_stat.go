package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ObjectAccessStatDB struct {
	*DB
}

func (db *DB) ObjectAccessStat() *ObjectAccessStatDB {
	return &ObjectAccessStatDB{db}
}

func (*ObjectAccessStatDB) Get(ctx SQLContext, objID cdssdk.ObjectID, stgID cdssdk.StorageID) (stgmod.ObjectAccessStat, error) {
	var ret stgmod.ObjectAccessStat
	err := ctx.Table("ObjectAccessStat").
		Where("ObjectID = ? AND StorageID = ?", objID, stgID).
		First(&ret).Error
	return ret, err
}

func (*ObjectAccessStatDB) GetByObjectID(ctx SQLContext, objID cdssdk.ObjectID) ([]stgmod.ObjectAccessStat, error) {
	var ret []stgmod.ObjectAccessStat
	err := ctx.Table("ObjectAccessStat").
		Where("ObjectID = ?", objID).
		Find(&ret).Error
	return ret, err
}

func (*ObjectAccessStatDB) BatchGetByObjectID(ctx SQLContext, objIDs []cdssdk.ObjectID) ([]stgmod.ObjectAccessStat, error) {
	if len(objIDs) == 0 {
		return nil, nil
	}

	var ret []stgmod.ObjectAccessStat
	err := ctx.Table("ObjectAccessStat").
		Where("ObjectID IN ?", objIDs).
		Find(&ret).Error
	return ret, err
}

func (*ObjectAccessStatDB) BatchGetByObjectIDOnStorage(ctx SQLContext, objIDs []cdssdk.ObjectID, stgID cdssdk.StorageID) ([]stgmod.ObjectAccessStat, error) {
	if len(objIDs) == 0 {
		return nil, nil
	}

	var ret []stgmod.ObjectAccessStat
	err := ctx.Table("ObjectAccessStat").
		Where("ObjectID IN ? AND StorageID = ?", objIDs, stgID).
		Find(&ret).Error
	return ret, err
}

func (*ObjectAccessStatDB) BatchAddCounter(ctx SQLContext, entries []coormq.AddAccessStatEntry) error {
	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		acc := stgmod.ObjectAccessStat{
			ObjectID:  entry.ObjectID,
			StorageID: entry.StorageID,
			Counter:   entry.Counter,
		}

		err := ctx.Table("ObjectAccessStat").
			Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "ObjectID"}, {Name: "StorageID"}},
				DoUpdates: clause.Assignments(map[string]any{
					"Counter": gorm.Expr("Counter + values(Counter)"),
				}),
			}).Create(&acc).Error
		if err != nil {
			return err
		}
	}
	return nil
}

func (*ObjectAccessStatDB) BatchUpdateAmountInPackage(ctx SQLContext, pkgIDs []cdssdk.PackageID, historyWeight float64) error {
	if len(pkgIDs) == 0 {
		return nil
	}

	err := ctx.Exec("UPDATE ObjectAccessStat AS o INNER JOIN Object AS obj ON o.ObjectID = obj.ObjectID SET o.Amount = o.Amount * ? + o.Counter * (1 - ?), o.Counter = 0 WHERE obj.PackageID IN ?", historyWeight, historyWeight, pkgIDs).Error
	return err
}

func (*ObjectAccessStatDB) UpdateAllAmount(ctx SQLContext, historyWeight float64) error {
	err := ctx.Exec("UPDATE ObjectAccessStat SET Amount = Amount * ? + Counter * (1 - ?), Counter = 0", historyWeight, historyWeight).Error
	return err
}

func (*ObjectAccessStatDB) DeleteByObjectID(ctx SQLContext, objID cdssdk.ObjectID) error {
	err := ctx.Table("ObjectAccessStat").Where("ObjectID = ?", objID).Delete(nil).Error
	return err
}

func (*ObjectAccessStatDB) BatchDeleteByObjectID(ctx SQLContext, objIDs []cdssdk.ObjectID) error {
	if len(objIDs) == 0 {
		return nil
	}

	err := ctx.Table("ObjectAccessStat").Where("ObjectID IN ?", objIDs).Delete(nil).Error
	return err
}

func (*ObjectAccessStatDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	err := ctx.Exec("DELETE o FROM ObjectAccessStat o INNER JOIN Object obj ON o.ObjectID = obj.ObjectID WHERE obj.PackageID = ?", packageID).Error
	return err
}
