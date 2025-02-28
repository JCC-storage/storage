package db2

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm/clause"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type ObjectDB struct {
	*DB
}

func (db *DB) Object() *ObjectDB {
	return &ObjectDB{DB: db}
}

func (db *ObjectDB) GetByID(ctx SQLContext, objectID cdssdk.ObjectID) (cdssdk.Object, error) {
	var ret cdssdk.Object
	err := ctx.Table("Object").Where("ObjectID = ?", objectID).First(&ret).Error
	return ret, err
}

func (db *ObjectDB) GetByPath(ctx SQLContext, packageID cdssdk.PackageID, path string) ([]cdssdk.Object, error) {
	var ret []cdssdk.Object
	err := ctx.Table("Object").Where("PackageID = ? AND Path = ?", packageID, path).Find(&ret).Error
	return ret, err
}

func (db *ObjectDB) GetWithPathPrefix(ctx SQLContext, packageID cdssdk.PackageID, pathPrefix string) ([]cdssdk.Object, error) {
	var ret []cdssdk.Object
	err := ctx.Table("Object").Where("PackageID = ? AND Path LIKE ?", packageID, pathPrefix+"%").Order("ObjectID ASC").Find(&ret).Error
	return ret, err
}

func (db *ObjectDB) GetCommonPrefixes(ctx SQLContext, packageID cdssdk.PackageID, pathPrefix string) ([]string, error) {
	var ret []string

	sepCnt := strings.Count(pathPrefix, cdssdk.ObjectPathSeparator) + 1

	prefixStatm := fmt.Sprintf("Substring_Index(Path, '%s', %d)", cdssdk.ObjectPathSeparator, sepCnt)

	err := ctx.Table("Object").Select(prefixStatm+" as Prefix").
		Where("PackageID = ?", packageID).
		Where("Path like ?", pathPrefix+"%").
		Where(prefixStatm + " <> Path").
		Group("Prefix").Find(&ret).Error
	if err != nil {
		return nil, err
	}

	for i := range ret {
		ret[i] = ret[i] + cdssdk.ObjectPathSeparator
	}

	return ret, nil
}

func (db *ObjectDB) GetDirectChildren(ctx SQLContext, packageID cdssdk.PackageID, pathPrefix string) ([]cdssdk.Object, error) {
	var ret []cdssdk.Object

	sepCnt := strings.Count(pathPrefix, cdssdk.ObjectPathSeparator) + 1

	prefixStatm := fmt.Sprintf("Substring_Index(Path, '%s', %d)", cdssdk.ObjectPathSeparator, sepCnt)

	err := ctx.Table("Object").
		Where("PackageID = ?", packageID).
		Where("Path like ?", pathPrefix+"%").
		Where(prefixStatm + " = Path").
		Find(&ret).Error
	return ret, err
}

func (db *ObjectDB) BatchTestObjectID(ctx SQLContext, objectIDs []cdssdk.ObjectID) (map[cdssdk.ObjectID]bool, error) {
	if len(objectIDs) == 0 {
		return make(map[cdssdk.ObjectID]bool), nil
	}

	var avaiIDs []cdssdk.ObjectID
	err := ctx.Table("Object").Where("ObjectID IN ?", objectIDs).Pluck("ObjectID", &avaiIDs).Error
	if err != nil {
		return nil, err
	}

	avaiIDMap := make(map[cdssdk.ObjectID]bool)
	for _, pkgID := range avaiIDs {
		avaiIDMap[pkgID] = true
	}

	return avaiIDMap, nil
}

func (db *ObjectDB) BatchGet(ctx SQLContext, objectIDs []cdssdk.ObjectID) ([]cdssdk.Object, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}

	var objs []cdssdk.Object
	err := ctx.Table("Object").Where("ObjectID IN ?", objectIDs).Order("ObjectID ASC").Find(&objs).Error
	if err != nil {
		return nil, err
	}

	return objs, nil
}

func (db *ObjectDB) BatchGetByPackagePath(ctx SQLContext, pkgID cdssdk.PackageID, pathes []string) ([]cdssdk.Object, error) {
	if len(pathes) == 0 {
		return nil, nil
	}

	var objs []cdssdk.Object
	err := ctx.Table("Object").Where("PackageID = ? AND Path IN ?", pkgID, pathes).Find(&objs).Error
	if err != nil {
		return nil, err
	}

	return objs, nil
}

// 仅返回查询到的对象
func (db *ObjectDB) BatchGetDetails(ctx SQLContext, objectIDs []cdssdk.ObjectID) ([]stgmod.ObjectDetail, error) {
	var objs []cdssdk.Object

	err := ctx.Table("Object").Where("ObjectID IN ?", objectIDs).Order("ObjectID ASC").Find(&objs).Error
	if err != nil {
		return nil, err
	}

	// 获取所有的 ObjectBlock
	var allBlocks []stgmod.ObjectBlock
	err = ctx.Table("ObjectBlock").Where("ObjectID IN ?", objectIDs).Order("ObjectID, `Index` ASC").Find(&allBlocks).Error
	if err != nil {
		return nil, err
	}

	// 获取所有的 PinnedObject
	var allPinnedObjs []cdssdk.PinnedObject
	err = ctx.Table("PinnedObject").Where("ObjectID IN ?", objectIDs).Order("ObjectID ASC").Find(&allPinnedObjs).Error
	if err != nil {
		return nil, err
	}

	details := make([]stgmod.ObjectDetail, len(objs))
	for i, obj := range objs {
		details[i] = stgmod.ObjectDetail{
			Object: obj,
		}
	}

	stgmod.DetailsFillObjectBlocks(details, allBlocks)
	stgmod.DetailsFillPinnedAt(details, allPinnedObjs)
	return details, nil
}

func (db *ObjectDB) Create(ctx SQLContext, obj cdssdk.Object) (cdssdk.ObjectID, error) {
	err := ctx.Table("Object").Create(&obj).Error
	if err != nil {
		return 0, fmt.Errorf("insert object failed, err: %w", err)
	}
	return obj.ObjectID, nil
}

// 批量创建对象，创建完成后会填充ObjectID。
func (db *ObjectDB) BatchCreate(ctx SQLContext, objs *[]cdssdk.Object) error {
	if len(*objs) == 0 {
		return nil
	}

	return ctx.Table("Object").Create(objs).Error
}

// 批量更新对象所有属性，objs中的对象必须包含ObjectID
func (db *ObjectDB) BatchUpdate(ctx SQLContext, objs []cdssdk.Object) error {
	if len(objs) == 0 {
		return nil
	}

	return ctx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "ObjectID"}},
		UpdateAll: true,
	}).Create(objs).Error
}

// 批量更新对象指定属性，objs中的对象只需设置需要更新的属性即可，但：
//  1. 必须包含ObjectID
//  2. 日期类型属性不能设置为0值
func (db *ObjectDB) BatchUpdateColumns(ctx SQLContext, objs []cdssdk.Object, columns []string) error {
	if len(objs) == 0 {
		return nil
	}

	return ctx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "ObjectID"}},
		DoUpdates: clause.AssignmentColumns(columns),
	}).Create(objs).Error
}

func (db *ObjectDB) GetPackageObjects(ctx SQLContext, packageID cdssdk.PackageID) ([]cdssdk.Object, error) {
	var ret []cdssdk.Object
	err := ctx.Table("Object").Where("PackageID = ?", packageID).Order("ObjectID ASC").Find(&ret).Error
	return ret, err
}

func (db *ObjectDB) GetPackageObjectDetails(ctx SQLContext, packageID cdssdk.PackageID) ([]stgmod.ObjectDetail, error) {
	var objs []cdssdk.Object
	err := ctx.Table("Object").Where("PackageID = ?", packageID).Order("ObjectID ASC").Find(&objs).Error
	if err != nil {
		return nil, fmt.Errorf("getting objects: %w", err)
	}

	// 获取所有的 ObjectBlock
	var allBlocks []stgmod.ObjectBlock
	err = ctx.Table("ObjectBlock").
		Select("ObjectBlock.*").
		Joins("JOIN Object ON ObjectBlock.ObjectID = Object.ObjectID").
		Where("Object.PackageID = ?", packageID).
		Order("ObjectBlock.ObjectID, `Index` ASC").
		Find(&allBlocks).Error
	if err != nil {
		return nil, fmt.Errorf("getting all object blocks: %w", err)
	}

	// 获取所有的 PinnedObject
	var allPinnedObjs []cdssdk.PinnedObject
	err = ctx.Table("PinnedObject").
		Select("PinnedObject.*").
		Joins("JOIN Object ON PinnedObject.ObjectID = Object.ObjectID").
		Where("Object.PackageID = ?", packageID).
		Order("PinnedObject.ObjectID").
		Find(&allPinnedObjs).Error
	if err != nil {
		return nil, fmt.Errorf("getting all pinned objects: %w", err)
	}

	details := make([]stgmod.ObjectDetail, len(objs))
	for i, obj := range objs {
		details[i] = stgmod.ObjectDetail{
			Object: obj,
		}
	}

	stgmod.DetailsFillObjectBlocks(details, allBlocks)
	stgmod.DetailsFillPinnedAt(details, allPinnedObjs)
	return details, nil
}

func (db *ObjectDB) GetObjectsIfAnyBlockOnStorage(ctx SQLContext, stgID cdssdk.StorageID) ([]cdssdk.Object, error) {
	var objs []cdssdk.Object
	err := ctx.Table("Object").Where("ObjectID IN (SELECT ObjectID FROM ObjectBlock WHERE StorageID = ?)", stgID).Order("ObjectID ASC").Find(&objs).Error
	if err != nil {
		return nil, fmt.Errorf("getting objects: %w", err)
	}

	return objs, nil
}

func (db *ObjectDB) BatchAdd(ctx SQLContext, packageID cdssdk.PackageID, adds []coormq.AddObjectEntry) ([]cdssdk.Object, error) {
	if len(adds) == 0 {
		return nil, nil
	}

	// 收集所有路径
	pathes := make([]string, 0, len(adds))
	for _, add := range adds {
		pathes = append(pathes, add.Path)
	}

	// 先查询要更新的对象，不存在也没关系
	existsObjs, err := db.BatchGetByPackagePath(ctx, packageID, pathes)
	if err != nil {
		return nil, fmt.Errorf("batch get object by path: %w", err)
	}

	existsObjsMap := make(map[string]cdssdk.Object)
	for _, obj := range existsObjs {
		existsObjsMap[obj.Path] = obj
	}

	var updatingObjs []cdssdk.Object
	var addingObjs []cdssdk.Object
	for i := range adds {
		o := cdssdk.Object{
			PackageID:  packageID,
			Path:       adds[i].Path,
			Size:       adds[i].Size,
			FileHash:   adds[i].FileHash,
			Redundancy: cdssdk.NewNoneRedundancy(), // 首次上传默认使用不分块的none模式
			CreateTime: adds[i].UploadTime,
			UpdateTime: adds[i].UploadTime,
		}

		e, ok := existsObjsMap[adds[i].Path]
		if ok {
			o.ObjectID = e.ObjectID
			o.CreateTime = e.CreateTime
			updatingObjs = append(updatingObjs, o)

		} else {
			addingObjs = append(addingObjs, o)
		}
	}

	// 先进行更新
	err = db.BatchUpdate(ctx, updatingObjs)
	if err != nil {
		return nil, fmt.Errorf("batch update objects: %w", err)
	}

	// 再执行插入，Create函数插入后会填充ObjectID
	err = db.BatchCreate(ctx, &addingObjs)
	if err != nil {
		return nil, fmt.Errorf("batch create objects: %w", err)
	}

	// 按照add参数的顺序返回结果
	affectedObjsMp := make(map[string]cdssdk.Object)
	for _, o := range updatingObjs {
		affectedObjsMp[o.Path] = o
	}
	for _, o := range addingObjs {
		affectedObjsMp[o.Path] = o
	}
	affectedObjs := make([]cdssdk.Object, 0, len(affectedObjsMp))
	affectedObjIDs := make([]cdssdk.ObjectID, 0, len(affectedObjsMp))
	for i := range adds {
		obj := affectedObjsMp[adds[i].Path]
		affectedObjs = append(affectedObjs, obj)
		affectedObjIDs = append(affectedObjIDs, obj.ObjectID)
	}

	if len(affectedObjIDs) > 0 {
		// 批量删除 ObjectBlock
		if err := db.ObjectBlock().BatchDeleteByObjectID(ctx, affectedObjIDs); err != nil {
			return nil, fmt.Errorf("batch delete object blocks: %w", err)
		}

		// 批量删除 PinnedObject
		if err := db.PinnedObject().BatchDeleteByObjectID(ctx, affectedObjIDs); err != nil {
			return nil, fmt.Errorf("batch delete pinned objects: %w", err)
		}
	}

	// 创建 ObjectBlock
	objBlocks := make([]stgmod.ObjectBlock, 0, len(adds))
	for i, add := range adds {
		for _, stgID := range add.StorageIDs {
			objBlocks = append(objBlocks, stgmod.ObjectBlock{
				ObjectID:  affectedObjIDs[i],
				Index:     0,
				StorageID: stgID,
				FileHash:  add.FileHash,
			})
		}
	}
	if err := db.ObjectBlock().BatchCreate(ctx, objBlocks); err != nil {
		return nil, fmt.Errorf("batch create object blocks: %w", err)
	}

	// 创建 Cache
	caches := make([]model.Cache, 0, len(adds))
	for _, add := range adds {
		for _, stgID := range add.StorageIDs {
			caches = append(caches, model.Cache{
				FileHash:   add.FileHash,
				StorageID:  stgID,
				CreateTime: time.Now(),
				Priority:   0,
			})
		}
	}
	if err := db.Cache().BatchCreate(ctx, caches); err != nil {
		return nil, fmt.Errorf("batch create caches: %w", err)
	}

	return affectedObjs, nil
}

func (db *ObjectDB) BatchDelete(ctx SQLContext, ids []cdssdk.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}

	return ctx.Table("Object").Where("ObjectID IN ?", ids).Delete(&cdssdk.Object{}).Error
}

func (db *ObjectDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	return ctx.Table("Object").Where("PackageID = ?", packageID).Delete(&cdssdk.Object{}).Error
}
