package db

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
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
	err := sqlx.Get(ctx, &ret, "select * from Object where ObjectID = ?", objectID)
	return ret.ToObject(), err
}

func (db *ObjectDB) BatchGetPackageObjectIDs(ctx SQLContext, pkgID cdssdk.PackageID, pathes []string) ([]cdssdk.ObjectID, error) {
	if len(pathes) == 0 {
		return nil, nil
	}

	// TODO In语句
	stmt, args, err := sqlx.In("select ObjectID from Object force index(PackagePath) where PackageID=? and Path in (?)", pkgID, pathes)
	if err != nil {
		return nil, err
	}
	stmt = ctx.Rebind(stmt)

	objIDs := make([]cdssdk.ObjectID, 0, len(pathes))
	err = sqlx.Select(ctx, &objIDs, stmt, args...)
	if err != nil {
		return nil, err
	}

	return objIDs, nil
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

// 可以用于批量创建或者更新记录
// 用于创建时，需要额外检查PackageID+Path的唯一性
// 用于更新时，需要额外检查现存的PackageID+Path对应的ObjectID是否与待更新的ObjectID相同。不会更新CreateTime。
func (db *ObjectDB) BatchCreateOrUpdate(ctx SQLContext, objs []cdssdk.Object) error {
	if len(objs) == 0 {
		return nil
	}

	sql := "insert into Object(PackageID, Path, Size, FileHash, Redundancy, CreateTime ,UpdateTime)" +
		" values(:PackageID,:Path,:Size,:FileHash,:Redundancy, :CreateTime, :UpdateTime) as new" +
		" on duplicate key update Size = new.Size, FileHash = new.FileHash, Redundancy = new.Redundancy, UpdateTime = new.UpdateTime"

	return BatchNamedExec(ctx, sql, 7, objs, nil)
}

func (*ObjectDB) GetPackageObjects(ctx SQLContext, packageID cdssdk.PackageID) ([]model.Object, error) {
	var ret []model.TempObject
	err := sqlx.Select(ctx, &ret, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	return lo.Map(ret, func(o model.TempObject, idx int) model.Object { return o.ToObject() }), err
}

// GetPackageObjectDetails 获取指定包ID的对象详情列表。
//
// ctx: SQL执行上下文。
// packageID: 指定的包ID。
//
// 返回值为Object详情列表和可能出现的错误。
func (db *ObjectDB) GetPackageObjectDetails(ctx SQLContext, packageID cdssdk.PackageID) ([]stgmod.ObjectDetail, error) {
	// 从Object表中查询所有属于指定包ID的对象，按ObjectID升序排序
	var objs []model.TempObject
	err := sqlx.Select(ctx, &objs, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	if err != nil {
		return nil, fmt.Errorf("getting objects: %w", err)
	}

	// 初始化返回的Object详情列表
	rets := make([]stgmod.ObjectDetail, 0, len(objs))

	// 从ObjectBlock表中查询所有属于指定包ID的对象块，按ObjectID和Index升序排序
	var allBlocks []stgmod.ObjectBlock
	err = sqlx.Select(ctx, &allBlocks, "select ObjectBlock.* from ObjectBlock, Object where PackageID = ? and ObjectBlock.ObjectID = Object.ObjectID order by ObjectBlock.ObjectID, `Index` asc", packageID)
	if err != nil {
		return nil, fmt.Errorf("getting all object blocks: %w", err)
	}

	// 从PinnedObject表中查询所有属于指定包ID的被固定的对象，按ObjectID排序
	var allPinnedObjs []cdssdk.PinnedObject
	err = sqlx.Select(ctx, &allPinnedObjs, "select PinnedObject.* from PinnedObject, Object where PackageID = ? and PinnedObject.ObjectID = Object.ObjectID order by PinnedObject.ObjectID", packageID)
	if err != nil {
		return nil, fmt.Errorf("getting all pinned objects: %w", err)
	}

	// 遍历查询得到的对象，为每个对象构建详细的Object信息
	blksCur := 0    // 当前遍历到的对象块索引
	pinnedsCur := 0 // 当前遍历到的被固定对象索引
	for _, temp := range objs {
		detail := stgmod.ObjectDetail{
			Object: temp.ToObject(),
		}

		// 同时遍历对象和对象块的结果集，将属于同一对象的对象块附加到Object详情中
		for ; blksCur < len(allBlocks); blksCur++ {
			if allBlocks[blksCur].ObjectID != temp.ObjectID {
				break
			}
			detail.Blocks = append(detail.Blocks, allBlocks[blksCur])
		}

		// 遍历被固定对象的结果集，将被固定的信息附加到Object详情中
		for ; pinnedsCur < len(allPinnedObjs); pinnedsCur++ {
			if allPinnedObjs[pinnedsCur].ObjectID != temp.ObjectID {
				break
			}
			detail.PinnedAt = append(detail.PinnedAt, allPinnedObjs[pinnedsCur].NodeID)
		}

		// 将构建好的Object详情添加到返回列表中
		rets = append(rets, detail)
	}

	return rets, nil
}

func (db *ObjectDB) BatchAdd(ctx SQLContext, packageID cdssdk.PackageID, adds []coormq.AddObjectEntry) ([]cdssdk.ObjectID, error) {
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

	err := db.BatchCreateOrUpdate(ctx, objs)
	if err != nil {
		return nil, fmt.Errorf("batch create or update objects: %w", err)
	}

	pathes := make([]string, 0, len(adds))
	for _, add := range adds {
		pathes = append(pathes, add.Path)
	}
	objIDs, err := db.BatchGetPackageObjectIDs(ctx, packageID, pathes)
	if err != nil {
		return nil, fmt.Errorf("batch get object ids: %w", err)
	}

	err = db.ObjectBlock().BatchDeleteByObjectID(ctx, objIDs)
	if err != nil {
		return nil, fmt.Errorf("batch delete object blocks: %w", err)
	}

	err = db.PinnedObject().BatchDeleteByObjectID(ctx, objIDs)
	if err != nil {
		return nil, fmt.Errorf("batch delete pinned objects: %w", err)
	}

	objBlocks := make([]stgmod.ObjectBlock, 0, len(adds))
	for i, add := range adds {
		objBlocks = append(objBlocks, stgmod.ObjectBlock{
			ObjectID: objIDs[i],
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

	return objIDs, nil
}

func (db *ObjectDB) BatchUpdateRedundancy(ctx SQLContext, objs []coormq.ChangeObjectRedundancyEntry) error {
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
