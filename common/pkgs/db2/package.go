package db2

import (
	"errors"
	"fmt"

	"gorm.io/gorm"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type PackageDB struct {
	*DB
}

func (db *DB) Package() *PackageDB {
	return &PackageDB{DB: db}
}

func (db *PackageDB) GetByID(ctx SQLContext, packageID cdssdk.PackageID) (model.Package, error) {
	var ret model.Package
	err := ctx.Table("Package").Where("PackageID = ?", packageID).First(&ret).Error
	return ret, err
}

func (db *PackageDB) GetByName(ctx SQLContext, bucketID cdssdk.BucketID, name string) (model.Package, error) {
	var ret model.Package
	err := ctx.Table("Package").Where("BucketID = ? AND Name = ?", bucketID, name).First(&ret).Error
	return ret, err
}

func (db *PackageDB) BatchTestPackageID(ctx SQLContext, pkgIDs []cdssdk.PackageID) (map[cdssdk.PackageID]bool, error) {
	if len(pkgIDs) == 0 {
		return make(map[cdssdk.PackageID]bool), nil
	}

	var avaiIDs []cdssdk.PackageID
	err := ctx.Table("Package").
		Select("PackageID").
		Where("PackageID IN ?", pkgIDs).
		Find(&avaiIDs).Error
	if err != nil {
		return nil, err
	}

	avaiIDMap := make(map[cdssdk.PackageID]bool)
	for _, pkgID := range avaiIDs {
		avaiIDMap[pkgID] = true
	}

	return avaiIDMap, nil
}

func (*PackageDB) BatchGetAllPackageIDs(ctx SQLContext, start int, count int) ([]cdssdk.PackageID, error) {
	var ret []cdssdk.PackageID
	err := ctx.Table("Package").Select("PackageID").Limit(count).Offset(start).Find(&ret).Error
	return ret, err
}

func (db *PackageDB) GetBucketPackages(ctx SQLContext, userID cdssdk.UserID, bucketID cdssdk.BucketID) ([]model.Package, error) {
	var ret []model.Package
	err := ctx.Table("UserBucket").
		Select("Package.*").
		Joins("JOIN Package ON UserBucket.BucketID = Package.BucketID").
		Where("UserBucket.UserID = ? AND UserBucket.BucketID = ?", userID, bucketID).
		Find(&ret).Error
	return ret, err
}

// IsAvailable 判断一个用户是否拥有指定对象
func (db *PackageDB) IsAvailable(ctx SQLContext, userID cdssdk.UserID, packageID cdssdk.PackageID) (bool, error) {
	var pkgID cdssdk.PackageID
	err := ctx.Table("Package").
		Select("Package.PackageID").
		Joins("JOIN UserBucket ON Package.BucketID = UserBucket.BucketID").
		Where("Package.PackageID = ? AND UserBucket.UserID = ?", packageID, userID).
		Scan(&pkgID).Error

	if err == gorm.ErrRecordNotFound {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("find package failed, err: %w", err)
	}

	return true, nil
}

// GetUserPackage 获得Package，如果用户没有权限访问，则不会获得结果
func (db *PackageDB) GetUserPackage(ctx SQLContext, userID cdssdk.UserID, packageID cdssdk.PackageID) (model.Package, error) {
	var ret model.Package
	err := ctx.Table("Package").
		Select("Package.*").
		Joins("JOIN UserBucket ON Package.BucketID = UserBucket.BucketID").
		Where("Package.PackageID = ? AND UserBucket.UserID = ?", packageID, userID).
		First(&ret).Error
	return ret, err
}

// 在指定名称的Bucket中查找指定名称的Package
func (*PackageDB) GetUserPackageByName(ctx SQLContext, userID cdssdk.UserID, bucketName string, packageName string) (model.Package, error) {
	var ret model.Package
	err := ctx.Table("Package").
		Select("Package.*").
		Joins("JOIN Bucket ON Package.BucketID = Bucket.BucketID").
		Joins("JOIN UserBucket ON Bucket.BucketID = UserBucket.BucketID").
		Where("Package.Name = ? AND Bucket.Name = ? AND UserBucket.UserID = ?", packageName, bucketName, userID).
		First(&ret).Error
	return ret, err
}

func (db *PackageDB) Create(ctx SQLContext, bucketID cdssdk.BucketID, name string) (cdssdk.PackageID, error) {
	var packageID int64
	err := ctx.Table("Package").
		Select("PackageID").
		Where("Name = ? AND BucketID = ?", name, bucketID).
		First(&packageID).Error

	if err == nil {
		return 0, fmt.Errorf("package with given Name and BucketID already exists")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, fmt.Errorf("query Package by PackageName and BucketID failed, err: %w", err)
	}

	newPackage := model.Package{Name: name, BucketID: bucketID, State: cdssdk.PackageStateNormal}
	if err := ctx.Create(&newPackage).Error; err != nil {
		return 0, fmt.Errorf("insert package failed, err: %w", err)
	}

	return cdssdk.PackageID(newPackage.PackageID), nil
}

// SoftDelete 设置一个对象被删除，并将相关数据删除
func (db *PackageDB) SoftDelete(ctx SQLContext, packageID cdssdk.PackageID) error {
	obj, err := db.GetByID(ctx, packageID)
	if err != nil {
		return fmt.Errorf("get package failed, err: %w", err)
	}

	if obj.State != cdssdk.PackageStateNormal {
		return nil
	}

	if err := db.ChangeState(ctx, packageID, cdssdk.PackageStateDeleted); err != nil {
		return fmt.Errorf("change package state failed, err: %w", err)
	}

	if err := db.ObjectAccessStat().DeleteInPackage(ctx, packageID); err != nil {
		return fmt.Errorf("delete from object access stat: %w", err)
	}

	if err := db.ObjectBlock().DeleteInPackage(ctx, packageID); err != nil {
		return fmt.Errorf("delete from object block failed, err: %w", err)
	}

	if err := db.PinnedObject().DeleteInPackage(ctx, packageID); err != nil {
		return fmt.Errorf("deleting pinned objects in package: %w", err)
	}

	if err := db.Object().DeleteInPackage(ctx, packageID); err != nil {
		return fmt.Errorf("deleting objects in package: %w", err)
	}

	if _, err := db.StoragePackage().SetAllPackageDeleted(ctx, packageID); err != nil {
		return fmt.Errorf("set storage package deleted failed, err: %w", err)
	}

	return nil
}

// DeleteUnused 删除一个已经是Deleted状态，且不再被使用的对象
func (PackageDB) DeleteUnused(ctx SQLContext, packageID cdssdk.PackageID) error {
	err := ctx.Exec("DELETE FROM Package WHERE PackageID = ? AND State = ? AND NOT EXISTS (SELECT StorageID FROM StoragePackage WHERE PackageID = ?)",
		packageID,
		cdssdk.PackageStateDeleted,
		packageID,
	).Error
	return err
}

func (*PackageDB) ChangeState(ctx SQLContext, packageID cdssdk.PackageID, state string) error {
	err := ctx.Exec("UPDATE Package SET State = ? WHERE PackageID = ?", state, packageID).Error
	return err
}
