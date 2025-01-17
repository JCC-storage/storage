package db2

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gorm.io/gorm/clause"
)

type PinnedObjectDB struct {
	*DB
}

func (db *DB) PinnedObject() *PinnedObjectDB {
	return &PinnedObjectDB{DB: db}
}

func (*PinnedObjectDB) GetByStorageID(ctx SQLContext, stgID cdssdk.StorageID) ([]cdssdk.PinnedObject, error) {
	var ret []cdssdk.PinnedObject
	err := ctx.Table("PinnedObject").Find(&ret, "StorageID = ?", stgID).Error
	return ret, err
}

func (*PinnedObjectDB) GetObjectsByStorageID(ctx SQLContext, stgID cdssdk.StorageID) ([]cdssdk.Object, error) {
	var ret []cdssdk.Object
	err := ctx.Table("Object").Joins("inner join PinnedObject on Object.ObjectID = PinnedObject.ObjectID").Where("StorageID = ?", stgID).Find(&ret).Error
	return ret, err
}

func (*PinnedObjectDB) Create(ctx SQLContext, stgID cdssdk.StorageID, objectID cdssdk.ObjectID, createTime time.Time) error {
	return ctx.Table("PinnedObject").Create(&cdssdk.PinnedObject{StorageID: stgID, ObjectID: objectID, CreateTime: createTime}).Error
}

func (*PinnedObjectDB) BatchGetByObjectID(ctx SQLContext, objectIDs []cdssdk.ObjectID) ([]cdssdk.PinnedObject, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}

	var pinneds []cdssdk.PinnedObject
	err := ctx.Table("PinnedObject").Where("ObjectID in (?)", objectIDs).Order("ObjectID asc").Find(&pinneds).Error
	return pinneds, err
}

func (*PinnedObjectDB) TryCreate(ctx SQLContext, stgID cdssdk.StorageID, objectID cdssdk.ObjectID, createTime time.Time) error {
	return ctx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "ObjectID"}, {Name: "StorageID"}},
		DoUpdates: clause.AssignmentColumns([]string{"CreateTime"}),
	}).Create(&cdssdk.PinnedObject{StorageID: stgID, ObjectID: objectID, CreateTime: createTime}).Error
}

func (*PinnedObjectDB) BatchTryCreate(ctx SQLContext, pinneds []cdssdk.PinnedObject) error {
	if len(pinneds) == 0 {
		return nil
	}

	return ctx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "ObjectID"}, {Name: "StorageID"}},
		DoUpdates: clause.AssignmentColumns([]string{"CreateTime"}),
	}).Create(&pinneds).Error
}

func (*PinnedObjectDB) CreateFromPackage(ctx SQLContext, packageID cdssdk.PackageID, stgID cdssdk.StorageID) error {
	err := ctx.Exec(
		"insert ignore into PinnedObject(StorageID, ObjectID, CreateTime) select ? as StorageID, ObjectID, ? as CreateTime from Object where PackageID = ?",
		stgID,
		time.Now(),
		packageID,
	).Error
	return err
}

func (db *PinnedObjectDB) ObjectBatchCreate(ctx SQLContext, objectID cdssdk.ObjectID, stgIDs []cdssdk.StorageID) error {
	if len(stgIDs) == 0 {
		return nil
	}

	for _, id := range stgIDs {
		err := db.TryCreate(ctx, id, objectID, time.Now())
		if err != nil {
			return err
		}
	}
	return nil
}

func (*PinnedObjectDB) Delete(ctx SQLContext, stgID cdssdk.StorageID, objectID cdssdk.ObjectID) error {
	err := ctx.Exec("delete from PinnedObject where StorageID = ? and ObjectID = ?", stgID, objectID).Error
	return err
}

func (*PinnedObjectDB) DeleteByObjectID(ctx SQLContext, objectID cdssdk.ObjectID) error {
	err := ctx.Exec("delete from PinnedObject where ObjectID = ?", objectID).Error
	return err
}

func (*PinnedObjectDB) BatchDeleteByObjectID(ctx SQLContext, objectIDs []cdssdk.ObjectID) error {
	if len(objectIDs) == 0 {
		return nil
	}

	err := ctx.Table("PinnedObject").Where("ObjectID in (?)", objectIDs).Delete(&cdssdk.PinnedObject{}).Error
	return err
}

func (*PinnedObjectDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	err := ctx.Table("PinnedObject").Where("ObjectID in (select ObjectID from Object where PackageID = ?)", packageID).Delete(&cdssdk.PinnedObject{}).Error
	return err
}

func (*PinnedObjectDB) DeleteInPackageAtStorage(ctx SQLContext, packageID cdssdk.PackageID, stgID cdssdk.StorageID) error {
	err := ctx.Exec("delete PinnedObject from PinnedObject inner join Object on PinnedObject.ObjectID = Object.ObjectID where PackageID = ? and StorageID = ?", packageID, stgID).Error
	return err
}

func (*PinnedObjectDB) StorageBatchDelete(ctx SQLContext, stgID cdssdk.StorageID, objectIDs []cdssdk.ObjectID) error {
	if len(objectIDs) == 0 {
		return nil
	}

	err := ctx.Table("PinnedObject").Where("StorageID = ? and ObjectID in (?)", stgID, objectIDs).Delete(&cdssdk.PinnedObject{}).Error
	return err
}
