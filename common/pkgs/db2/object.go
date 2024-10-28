package db2

import (
	"fmt"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"strings"
	"time"

	"github.com/samber/lo"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type ObjectDB struct {
	*DB
}

func (db *DB) Object() *ObjectDB {
	return &ObjectDB{DB: db}
}

func (db *ObjectDB) GetByID(ctx SQLContext, objectID cdssdk.ObjectID) (model.Object, error) {
	var ret model.TempObject
	err := ctx.Table("Object").Where("ObjectID = ?", objectID).First(&ret).Error
	return ret.ToObject(), err
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

func (db *ObjectDB) BatchGet(ctx SQLContext, objectIDs []cdssdk.ObjectID) ([]model.Object, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}

	var objs []model.TempObject
	err := ctx.Table("Object").Where("ObjectID IN ?", objectIDs).Order("ObjectID ASC").Find(&objs).Error
	if err != nil {
		return nil, err
	}

	return lo.Map(objs, func(o model.TempObject, idx int) cdssdk.Object { return o.ToObject() }), nil
}

func (db *ObjectDB) BatchGetByPackagePath(ctx SQLContext, pkgID cdssdk.PackageID, pathes []string) ([]cdssdk.Object, error) {
	if len(pathes) == 0 {
		return nil, nil
	}

	var objs []model.TempObject
	err := ctx.Table("Object").Where("PackageID = ? AND Path IN ?", pkgID, pathes).Find(&objs).Error
	if err != nil {
		return nil, err
	}

	return lo.Map(objs, func(o model.TempObject, idx int) cdssdk.Object { return o.ToObject() }), nil
}

func (db *ObjectDB) Create(ctx SQLContext, obj cdssdk.Object) (cdssdk.ObjectID, error) {
	err := ctx.Table("Object").Create(&obj).Error
	if err != nil {
		return 0, fmt.Errorf("insert object failed, err: %w", err)
	}
	return obj.ObjectID, nil
}

func (db *ObjectDB) BatchUpsertByPackagePath(ctx SQLContext, objs []cdssdk.Object) error {
	if len(objs) == 0 {
		return nil
	}

	// 使用 GORM 的 Save 方法，插入或更新对象
	return ctx.Table("Object").Save(&objs).Error
}

func (db *ObjectDB) BatchUpert(ctx SQLContext, objs []cdssdk.Object) error {
	if len(objs) == 0 {
		return nil
	}

	// 直接更新或插入
	return ctx.Table("Object").Save(&objs).Error
}

func (db *ObjectDB) GetPackageObjects(ctx SQLContext, packageID cdssdk.PackageID) ([]model.Object, error) {
	var ret []model.TempObject
	err := ctx.Table("Object").Where("PackageID = ?", packageID).Order("ObjectID ASC").Find(&ret).Error
	return lo.Map(ret, func(o model.TempObject, idx int) model.Object { return o.ToObject() }), err
}

func (db *ObjectDB) GetPackageObjectDetails(ctx SQLContext, packageID cdssdk.PackageID) ([]stgmod.ObjectDetail, error) {
	var objs []model.TempObject
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
			Object: obj.ToObject(),
		}
	}

	stgmod.DetailsFillObjectBlocks(details, allBlocks)
	stgmod.DetailsFillPinnedAt(details, allPinnedObjs)
	return details, nil
}

func (db *ObjectDB) GetObjectsIfAnyBlockOnNode(ctx SQLContext, nodeID cdssdk.NodeID) ([]cdssdk.Object, error) {
	var temps []model.TempObject
	err := ctx.Table("Object").Where("ObjectID IN (SELECT ObjectID FROM ObjectBlock WHERE NodeID = ?)", nodeID).Order("ObjectID ASC").Find(&temps).Error
	if err != nil {
		return nil, fmt.Errorf("getting objects: %w", err)
	}

	objs := make([]cdssdk.Object, len(temps))
	for i := range temps {
		objs[i] = temps[i].ToObject()
	}

	return objs, nil
}

func (db *ObjectDB) BatchAdd(ctx SQLContext, packageID cdssdk.PackageID, adds []coormq.AddObjectEntry) ([]cdssdk.Object, error) {
	if len(adds) == 0 {
		return nil, nil
	}

	objs := make([]cdssdk.Object, 0, len(adds))
	for _, add := range adds {
		objs = append(objs, cdssdk.Object{
			PackageID:  packageID,
			Path:       add.Path,
			Size:       add.Size,
			FileHash:   add.FileHash,
			Redundancy: cdssdk.NewNoneRedundancy(), // 首次上传默认使用不分块的none模式
			CreateTime: add.UploadTime,
			UpdateTime: add.UploadTime,
		})
	}

	err := db.BatchUpsertByPackagePath(ctx, objs)
	if err != nil {
		return nil, fmt.Errorf("batch create or update objects: %w", err)
	}

	// 收集所有路径
	pathes := make([]string, 0, len(adds))
	for _, add := range adds {
		pathes = append(pathes, add.Path)
	}

	// 批量获取对象
	addedObjs := []cdssdk.Object{}
	err = ctx.Table("Object").Where("PackageID = ? AND Path IN ?", packageID, pathes).Find(&addedObjs).Error
	if err != nil {
		return nil, fmt.Errorf("batch get object ids: %w", err)
	}

	// 对添加的对象和获取的对象进行排序
	adds = sort2.Sort(adds, func(l, r coormq.AddObjectEntry) int { return strings.Compare(l.Path, r.Path) })
	addedObjs = sort2.Sort(addedObjs, func(l, r cdssdk.Object) int { return strings.Compare(l.Path, r.Path) })

	// 收集对象 ID
	addedObjIDs := make([]cdssdk.ObjectID, len(addedObjs))
	for i := range addedObjs {
		addedObjIDs[i] = addedObjs[i].ObjectID
	}

	// 批量删除 ObjectBlock
	if err := ctx.Table("ObjectBlock").Where("ObjectID IN ?", addedObjIDs).Delete(&stgmod.ObjectBlock{}).Error; err != nil {
		return nil, fmt.Errorf("batch delete object blocks: %w", err)
	}

	// 批量删除 PinnedObject
	if err := ctx.Table("PinnedObject").Where("ObjectID IN ?", addedObjIDs).Delete(&cdssdk.PinnedObject{}).Error; err != nil {
		return nil, fmt.Errorf("batch delete pinned objects: %w", err)
	}

	// 创建 ObjectBlock
	objBlocks := make([]stgmod.ObjectBlock, len(adds))
	for i, add := range adds {
		objBlocks[i] = stgmod.ObjectBlock{
			ObjectID: addedObjIDs[i],
			Index:    0,
			NodeID:   add.NodeID,
			FileHash: add.FileHash,
		}
	}
	if err := ctx.Table("ObjectBlock").Create(&objBlocks).Error; err != nil {
		return nil, fmt.Errorf("batch create object blocks: %w", err)
	}

	// 创建 Cache
	caches := make([]model.Cache, len(adds))
	for _, add := range adds {
		caches = append(caches, model.Cache{
			FileHash:   add.FileHash,
			NodeID:     add.NodeID,
			CreateTime: time.Now(),
			Priority:   0,
		})
	}
	if err := ctx.Table("Cache").Create(&caches).Error; err != nil {
		return nil, fmt.Errorf("batch create caches: %w", err)
	}

	return addedObjs, nil
}

func (db *ObjectDB) BatchUpdateRedundancy(ctx SQLContext, objs []coormq.UpdatingObjectRedundancy) error {
	if len(objs) == 0 {
		return nil
	}

	nowTime := time.Now()
	objIDs := make([]cdssdk.ObjectID, 0, len(objs))
	dummyObjs := make([]cdssdk.Object, 0, len(objs))
	for _, obj := range objs {
		objIDs = append(objIDs, obj.ObjectID)
		dummyObjs = append(dummyObjs, cdssdk.Object{
			ObjectID:   obj.ObjectID,
			Redundancy: obj.Redundancy,
			CreateTime: nowTime,
			UpdateTime: nowTime,
		})
	}

	// 目前只能使用这种方式来同时更新大量数据
	err := BatchNamedExec(ctx,
		"insert into Object(ObjectID, PackageID, Path, Size, FileHash, Redundancy, CreateTime, UpdateTime)"+
			" values(:ObjectID, :PackageID, :Path, :Size, :FileHash, :Redundancy, :CreateTime, :UpdateTime) as new"+
			" on duplicate key update Redundancy=new.Redundancy", 8, dummyObjs, nil)
	if err != nil {
		return fmt.Errorf("batch update object redundancy: %w", err)
	}

	// 删除原本所有的编码块记录，重新添加
	err = db.ObjectBlock().BatchDeleteByObjectID(ctx, objIDs)
	if err != nil {
		return fmt.Errorf("batch delete object blocks: %w", err)
	}

	// 删除原本Pin住的Object。暂不考虑FileHash没有变化的情况
	err = db.PinnedObject().BatchDeleteByObjectID(ctx, objIDs)
	if err != nil {
		return fmt.Errorf("batch delete pinned object: %w", err)
	}

	blocks := make([]stgmod.ObjectBlock, 0, len(objs))
	for _, obj := range objs {
		blocks = append(blocks, obj.Blocks...)
	}
	err = db.ObjectBlock().BatchCreate(ctx, blocks)
	if err != nil {
		return fmt.Errorf("batch create object blocks: %w", err)
	}

	caches := make([]model.Cache, 0, len(objs))
	for _, obj := range objs {
		for _, blk := range obj.Blocks {
			caches = append(caches, model.Cache{
				FileHash:   blk.FileHash,
				NodeID:     blk.NodeID,
				CreateTime: time.Now(),
				Priority:   0,
			})
		}
	}
	err = db.Cache().BatchCreate(ctx, caches)
	if err != nil {
		return fmt.Errorf("batch create object caches: %w", err)
	}

	pinneds := make([]cdssdk.PinnedObject, 0, len(objs))
	for _, obj := range objs {
		for _, p := range obj.PinnedAt {
			pinneds = append(pinneds, cdssdk.PinnedObject{
				ObjectID:   obj.ObjectID,
				StorageID:  p,
				CreateTime: time.Now(),
			})
		}
	}
	err = db.PinnedObject().BatchTryCreate(ctx, pinneds)
	if err != nil {
		return fmt.Errorf("batch create pinned objects: %w", err)
	}

	return nil
}

func (db *ObjectDB) BatchDelete(ctx SQLContext, ids []cdssdk.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}

	return ctx.Table("Object").Where("ObjectID IN ?", ids).Delete(&model.TempObject{}).Error
}

func (db *ObjectDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	return ctx.Table("Object").Where("PackageID = ?", packageID).Delete(&model.TempObject{}).Error
}
