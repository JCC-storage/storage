package db

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"
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
	err := sqlx.Get(ctx, &ret, "select * from Object where ObjectID = ?", objectID)
	return ret.ToObject(), err
}

func (db *ObjectDB) BatchTestObjectID(ctx SQLContext, objectIDs []cdssdk.ObjectID) (map[cdssdk.ObjectID]bool, error) {
	if len(objectIDs) == 0 {
		return make(map[cdssdk.ObjectID]bool), nil
	}

	stmt, args, err := sqlx.In("select ObjectID from Object where ObjectID in (?)", lo.Uniq(objectIDs))
	if err != nil {
		return nil, err
	}

	var avaiIDs []cdssdk.ObjectID
	err = sqlx.Select(ctx, &avaiIDs, stmt, args...)
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

	// TODO In语句
	stmt, args, err := sqlx.In("select * from Object where ObjectID in (?) order by ObjectID asc", objectIDs)
	if err != nil {
		return nil, err
	}
	stmt = ctx.Rebind(stmt)

	objs := make([]model.TempObject, 0, len(objectIDs))
	err = sqlx.Select(ctx, &objs, stmt, args...)
	if err != nil {
		return nil, err
	}

	return lo.Map(objs, func(o model.TempObject, idx int) cdssdk.Object { return o.ToObject() }), nil
}

func (db *ObjectDB) BatchGetByPackagePath(ctx SQLContext, pkgID cdssdk.PackageID, pathes []string) ([]cdssdk.Object, error) {
	if len(pathes) == 0 {
		return nil, nil
	}

	// TODO In语句
	stmt, args, err := sqlx.In("select * from Object force index(PackagePath) where PackageID=? and Path in (?)", pkgID, pathes)
	if err != nil {
		return nil, err
	}
	stmt = ctx.Rebind(stmt)

	objs := make([]model.TempObject, 0, len(pathes))
	err = sqlx.Select(ctx, &objs, stmt, args...)
	if err != nil {
		return nil, err
	}

	return lo.Map(objs, func(o model.TempObject, idx int) cdssdk.Object { return o.ToObject() }), nil
}

func (db *ObjectDB) Create(ctx SQLContext, obj cdssdk.Object) (cdssdk.ObjectID, error) {
	sql := "insert into Object(PackageID, Path, Size, FileHash, Redundancy, CreateTime, UpdateTime) values(?,?,?,?,?,?,?)"

	ret, err := ctx.Exec(sql, obj.PackageID, obj.Path, obj.Size, obj.FileHash, obj.Redundancy, obj.UpdateTime, obj.UpdateTime)
	if err != nil {
		return 0, fmt.Errorf("insert object failed, err: %w", err)
	}

	objectID, err := ret.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get id of inserted object failed, err: %w", err)
	}

	return cdssdk.ObjectID(objectID), nil
}

// 可以用于批量创建或者更新记录。
// 用于创建时，需要额外检查PackageID+Path的唯一性。
// 用于更新时，需要额外检查现存的PackageID+Path对应的ObjectID是否与待更新的ObjectID相同。不会更新CreateTime。
func (db *ObjectDB) BatchUpsertByPackagePath(ctx SQLContext, objs []cdssdk.Object) error {
	if len(objs) == 0 {
		return nil
	}

	sql := "insert into Object(PackageID, Path, Size, FileHash, Redundancy, CreateTime ,UpdateTime)" +
		" values(:PackageID,:Path,:Size,:FileHash,:Redundancy, :CreateTime, :UpdateTime) as new" +
		" on duplicate key update Size = new.Size, FileHash = new.FileHash, Redundancy = new.Redundancy, UpdateTime = new.UpdateTime"

	return BatchNamedExec(ctx, sql, 7, objs, nil)
}

func (db *ObjectDB) BatchUpert(ctx SQLContext, objs []cdssdk.Object) error {
	if len(objs) == 0 {
		return nil
	}

	sql := "insert into Object(ObjectID, PackageID, Path, Size, FileHash, Redundancy, CreateTime ,UpdateTime)" +
		" values(:ObjectID, :PackageID,:Path,:Size,:FileHash,:Redundancy, :CreateTime, :UpdateTime) as new" +
		" on duplicate key update PackageID = new.PackageID, Path = new.Path, Size = new.Size, FileHash = new.FileHash, Redundancy = new.Redundancy, UpdateTime = new.UpdateTime"

	return BatchNamedExec(ctx, sql, 8, objs, nil)
}

func (*ObjectDB) GetPackageObjects(ctx SQLContext, packageID cdssdk.PackageID) ([]model.Object, error) {
	var ret []model.TempObject
	err := sqlx.Select(ctx, &ret, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	return lo.Map(ret, func(o model.TempObject, idx int) model.Object { return o.ToObject() }), err
}

func (db *ObjectDB) GetPackageObjectDetails(ctx SQLContext, packageID cdssdk.PackageID) ([]stgmod.ObjectDetail, error) {
	var objs []model.TempObject
	err := sqlx.Select(ctx, &objs, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	if err != nil {
		return nil, fmt.Errorf("getting objects: %w", err)
	}

	rets := make([]stgmod.ObjectDetail, 0, len(objs))

	var allBlocks []stgmod.ObjectBlock
	err = sqlx.Select(ctx, &allBlocks, "select ObjectBlock.* from ObjectBlock, Object where PackageID = ? and ObjectBlock.ObjectID = Object.ObjectID order by ObjectBlock.ObjectID, `Index` asc", packageID)
	if err != nil {
		return nil, fmt.Errorf("getting all object blocks: %w", err)
	}

	var allPinnedObjs []cdssdk.PinnedObject
	err = sqlx.Select(ctx, &allPinnedObjs, "select PinnedObject.* from PinnedObject, Object where PackageID = ? and PinnedObject.ObjectID = Object.ObjectID order by PinnedObject.ObjectID", packageID)
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
	return rets, nil
}

func (*ObjectDB) GetObjectsIfAnyBlockOnNode(ctx SQLContext, nodeID cdssdk.NodeID) ([]cdssdk.Object, error) {
	var temps []model.TempObject
	err := sqlx.Select(ctx, &temps, "select * from Object where ObjectID in (select ObjectID from ObjectBlock where NodeID = ?) order by ObjectID asc", nodeID)
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

	pathes := make([]string, 0, len(adds))
	for _, add := range adds {
		pathes = append(pathes, add.Path)
	}
	// 这里可以不用检查查询结果是否与pathes的数量相同
	addedObjs, err := db.BatchGetByPackagePath(ctx, packageID, pathes)
	if err != nil {
		return nil, fmt.Errorf("batch get object ids: %w", err)
	}

	// 所有需要按索引来一一对应的数据都需要进行排序
	adds = sort2.Sort(adds, func(l, r coormq.AddObjectEntry) int { return strings.Compare(l.Path, r.Path) })
	addedObjs = sort2.Sort(addedObjs, func(l, r cdssdk.Object) int { return strings.Compare(l.Path, r.Path) })

	addedObjIDs := make([]cdssdk.ObjectID, len(addedObjs))
	for i := range addedObjs {
		addedObjIDs[i] = addedObjs[i].ObjectID
	}

	err = db.ObjectBlock().BatchDeleteByObjectID(ctx, addedObjIDs)
	if err != nil {
		return nil, fmt.Errorf("batch delete object blocks: %w", err)
	}

	err = db.PinnedObject().BatchDeleteByObjectID(ctx, addedObjIDs)
	if err != nil {
		return nil, fmt.Errorf("batch delete pinned objects: %w", err)
	}

	objBlocks := make([]stgmod.ObjectBlock, 0, len(adds))
	for i, add := range adds {
		objBlocks = append(objBlocks, stgmod.ObjectBlock{
			ObjectID: addedObjIDs[i],
			Index:    0,
			NodeID:   add.NodeID,
			FileHash: add.FileHash,
		})
	}
	err = db.ObjectBlock().BatchCreate(ctx, objBlocks)
	if err != nil {
		return nil, fmt.Errorf("batch create object blocks: %w", err)
	}

	caches := make([]model.Cache, 0, len(adds))
	for _, add := range adds {
		caches = append(caches, model.Cache{
			FileHash:   add.FileHash,
			NodeID:     add.NodeID,
			CreateTime: time.Now(),
			Priority:   0,
		})
	}
	err = db.Cache().BatchCreate(ctx, caches)
	if err != nil {
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
				NodeID:     p,
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

func (*ObjectDB) BatchDelete(ctx SQLContext, ids []cdssdk.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}

	query, args, err := sqlx.In("delete from Object where ObjectID in (?)", ids)
	if err != nil {
		return err
	}

	_, err = ctx.Exec(query, args...)
	return err
}

func (*ObjectDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete from Object where PackageID = ?", packageID)
	return err
}
