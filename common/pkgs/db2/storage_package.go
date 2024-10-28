package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type StoragePackageDB struct {
	*DB
}

func (db *DB) StoragePackage() *StoragePackageDB {
	return &StoragePackageDB{DB: db}
}

func (*StoragePackageDB) Get(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID, userID cdssdk.UserID) (model.StoragePackage, error) {
	var ret model.StoragePackage
	err := ctx.Table("StoragePackage").Where("StorageID = ? AND PackageID = ? AND UserID = ?", storageID, packageID, userID).First(&ret).Error
	return ret, err
}

func (*StoragePackageDB) GetAllByStorageAndPackageID(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID) ([]model.StoragePackage, error) {
	var ret []model.StoragePackage
	err := ctx.Table("StoragePackage").Where("StorageID = ? AND PackageID = ?", storageID, packageID).Find(&ret).Error
	return ret, err
}

func (*StoragePackageDB) GetAllByStorageID(ctx SQLContext, storageID cdssdk.StorageID) ([]model.StoragePackage, error) {
	var ret []model.StoragePackage
	err := ctx.Table("StoragePackage").Where("StorageID = ?", storageID).Find(&ret).Error
	return ret, err
}

func (*StoragePackageDB) CreateOrUpdate(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID, userID cdssdk.UserID) error {
	sql := "INSERT INTO StoragePackage (StorageID, PackageID, UserID, State) VALUES (?, ?, ?, ?) " +
		"ON DUPLICATE KEY UPDATE State = VALUES(State)"
	return ctx.Exec(sql, storageID, packageID, userID, model.StoragePackageStateNormal).Error
}

func (*StoragePackageDB) ChangeState(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID, userID cdssdk.UserID, state string) error {
	return ctx.Table("StoragePackage").Where("StorageID = ? AND PackageID = ? AND UserID = ?", storageID, packageID, userID).Update("State", state).Error
}

// SetStateNormal 将状态设置为Normal，如果记录状态是Deleted，则不进行操作
func (*StoragePackageDB) SetStateNormal(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID, userID cdssdk.UserID) error {
	return ctx.Table("StoragePackage").Where("StorageID = ? AND PackageID = ? AND UserID = ? AND State <> ?",
		storageID, packageID, userID, model.StoragePackageStateDeleted).Update("State", model.StoragePackageStateNormal).Error
}

func (*StoragePackageDB) SetAllPackageState(ctx SQLContext, packageID cdssdk.PackageID, state string) (int64, error) {
	ret := ctx.Table("StoragePackage").Where("PackageID = ?", packageID).Update("State", state)
	if err := ret.Error; err != nil {
		return 0, err
	}
	return ret.RowsAffected, nil
}

// SetAllPackageOutdated 将Storage中指定对象设置为已过期。只会设置Normal状态的对象
func (*StoragePackageDB) SetAllPackageOutdated(ctx SQLContext, packageID cdssdk.PackageID) (int64, error) {
	ret := ctx.Table("StoragePackage").Where("State = ? AND PackageID = ?", model.StoragePackageStateNormal, packageID).Update("State", model.StoragePackageStateOutdated)
	if err := ret.Error; err != nil {
		return 0, err
	}
	return ret.RowsAffected, nil
}

func (db *StoragePackageDB) SetAllPackageDeleted(ctx SQLContext, packageID cdssdk.PackageID) (int64, error) {
	return db.SetAllPackageState(ctx, packageID, model.StoragePackageStateDeleted)
}

func (*StoragePackageDB) Delete(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID, userID cdssdk.UserID) error {
	return ctx.Table("StoragePackage").Where("StorageID = ? AND PackageID = ? AND UserID = ?", storageID, packageID, userID).Delete(&model.StoragePackage{}).Error
}

// FindPackageStorages 查询存储了指定对象的Storage
func (*StoragePackageDB) FindPackageStorages(ctx SQLContext, packageID cdssdk.PackageID) ([]model.Storage, error) {
	var ret []model.Storage
	err := ctx.Table("StoragePackage").Select("Storage.*").
		Joins("JOIN Storage ON StoragePackage.StorageID = Storage.StorageID").
		Where("PackageID = ?", packageID).
		Scan(&ret).Error
	return ret, err
}
