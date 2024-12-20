package db

import "gitlink.org.cn/cloudream/storage/datamap/internal/models"

type ObjectDB struct {
	*DB
}

func (db *DB) Object() *ObjectDB {
	return &ObjectDB{DB: db}
}

// GetAllObject 查询所有Object列表
func (*ObjectDB) GetAllObject(ctx SQLContext) ([]models.Object, error) {
	var ret []models.Object

	err := ctx.Table("object").Find(&ret).Error
	return ret, err
}

// GetObject 根据输入的ObjectId查询Object
func (*ObjectDB) GetObject(ctx SQLContext, objectId int64) (models.Object, error) {
	var ret models.Object
	err := ctx.Table("object").Where("ObjectID = ?", objectId).Find(&ret).Error
	return ret, err
}

// DeleteObject 根据输入的ObjectId删除Object
func (*ObjectDB) DeleteObject(ctx SQLContext, objectId int64) error {
	return ctx.Table("object").Where("ObjectID = ?", objectId).Delete(&models.Object{}).Error
}

// UpdateObject 根据输入的Object信息更新Object
func (*ObjectDB) UpdateObject(ctx SQLContext, object models.Object) error {
	return ctx.Table("object").Where("ObjectID = ?", object.ObjectID).Updates(&object).Error
}
