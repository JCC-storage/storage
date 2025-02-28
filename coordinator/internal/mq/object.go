package mq

import (
	"errors"
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gorm.io/gorm"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetObjects(msg *coormq.GetObjects) (*coormq.GetObjectsResp, *mq.CodeMessage) {
	var ret []*cdssdk.Object
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		// TODO 应该检查用户是否有每一个Object所在Package的权限
		objs, err := svc.db2.Object().BatchGet(tx, msg.ObjectIDs)
		if err != nil {
			return err
		}

		objMp := make(map[cdssdk.ObjectID]cdssdk.Object)
		for _, obj := range objs {
			objMp[obj.ObjectID] = obj
		}

		for _, objID := range msg.ObjectIDs {
			o, ok := objMp[objID]
			if ok {
				ret = append(ret, &o)
			} else {
				ret = append(ret, nil)
			}
		}

		return err
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warn(err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get objects failed")
	}

	return mq.ReplyOK(coormq.RespGetObjects(ret))
}

func (svc *Service) GetObjectsByPath(msg *coormq.GetObjectsByPath) (*coormq.GetObjectsByPathResp, *mq.CodeMessage) {
	var coms []string
	var objs []cdssdk.Object
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		var err error

		_, err = svc.db2.Package().GetUserPackage(tx, msg.UserID, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		if !msg.IsPrefix {
			objs, err = svc.db2.Object().GetByPath(tx, msg.PackageID, msg.Path)
			if err != nil {
				return fmt.Errorf("getting object by path: %w", err)
			}

			return nil
		}

		if !msg.NoRecursive {
			objs, err = svc.db2.Object().GetWithPathPrefix(tx, msg.PackageID, msg.Path)
			if err != nil {
				return fmt.Errorf("getting objects with prefix: %w", err)
			}
			return nil
		}

		coms, err = svc.db2.Object().GetCommonPrefixes(tx, msg.PackageID, msg.Path)
		if err != nil {
			return fmt.Errorf("getting common prefixes: %w", err)
		}

		objs, err = svc.db2.Object().GetDirectChildren(tx, msg.PackageID, msg.Path)
		if err != nil {
			return fmt.Errorf("getting direct children: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("PathPrefix", msg.Path).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get objects with prefix failed")
	}

	return mq.ReplyOK(coormq.RespGetObjectsByPath(coms, objs))
}

func (svc *Service) GetPackageObjects(msg *coormq.GetPackageObjects) (*coormq.GetPackageObjectsResp, *mq.CodeMessage) {
	var objs []cdssdk.Object
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		_, err := svc.db2.Package().GetUserPackage(tx, msg.UserID, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		objs, err = svc.db2.Object().GetPackageObjects(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package objects: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).WithField("PackageID", msg.PackageID).
			Warn(err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package objects failed")
	}

	return mq.ReplyOK(coormq.RespGetPackageObjects(objs))
}

func (svc *Service) GetPackageObjectDetails(msg *coormq.GetPackageObjectDetails) (*coormq.GetPackageObjectDetailsResp, *mq.CodeMessage) {
	var details []stgmod.ObjectDetail
	// 必须放在事务里进行，因为GetPackageBlockDetails是由多次数据库操作组成，必须保证数据的一致性
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		var err error
		_, err = svc.db2.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		details, err = svc.db2.Object().GetPackageObjectDetails(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package block details: %w", err)
		}

		return nil
	})

	if err != nil {
		logger.WithField("PackageID", msg.PackageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get package object block details failed")
	}

	return mq.ReplyOK(coormq.RespPackageObjectDetails(details))
}

func (svc *Service) GetObjectDetails(msg *coormq.GetObjectDetails) (*coormq.GetObjectDetailsResp, *mq.CodeMessage) {
	detailsMp := make(map[cdssdk.ObjectID]*stgmod.ObjectDetail)

	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		var err error

		msg.ObjectIDs = sort2.SortAsc(msg.ObjectIDs)

		// 根据ID依次查询Object，ObjectBlock，PinnedObject，并根据升序的特点进行合并
		objs, err := svc.db2.Object().BatchGet(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch get objects: %w", err)
		}
		for _, obj := range objs {
			detailsMp[obj.ObjectID] = &stgmod.ObjectDetail{
				Object: obj,
			}
		}

		// 查询合并
		blocks, err := svc.db2.ObjectBlock().BatchGetByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch get object blocks: %w", err)
		}
		for _, block := range blocks {
			d := detailsMp[block.ObjectID]
			d.Blocks = append(d.Blocks, block)
		}

		// 查询合并
		pinneds, err := svc.db2.PinnedObject().BatchGetByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch get pinned objects: %w", err)
		}
		for _, pinned := range pinneds {
			d := detailsMp[pinned.ObjectID]
			d.PinnedAt = append(d.PinnedAt, pinned.StorageID)
		}

		return nil
	})

	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get object details failed")
	}

	details := make([]*stgmod.ObjectDetail, len(msg.ObjectIDs))
	for i, objID := range msg.ObjectIDs {
		details[i] = detailsMp[objID]
	}

	return mq.ReplyOK(coormq.RespGetObjectDetails(details))
}

func (svc *Service) UpdateObjectRedundancy(msg *coormq.UpdateObjectRedundancy) (*coormq.UpdateObjectRedundancyResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(ctx db2.SQLContext) error {
		db := svc.db2
		objs := msg.Updatings

		nowTime := time.Now()
		objIDs := make([]cdssdk.ObjectID, 0, len(objs))
		for _, obj := range objs {
			objIDs = append(objIDs, obj.ObjectID)
		}

		avaiIDs, err := db.Object().BatchTestObjectID(ctx, objIDs)
		if err != nil {
			return fmt.Errorf("batch test object id: %w", err)
		}

		// 过滤掉已经不存在的对象。
		// 注意，objIDs没有被过滤，因为后续逻辑不过滤也不会出错
		objs = lo.Filter(objs, func(obj coormq.UpdatingObjectRedundancy, _ int) bool {
			return avaiIDs[obj.ObjectID]
		})

		dummyObjs := make([]cdssdk.Object, 0, len(objs))
		for _, obj := range objs {
			dummyObjs = append(dummyObjs, cdssdk.Object{
				ObjectID:   obj.ObjectID,
				Redundancy: obj.Redundancy,
				CreateTime: nowTime, // 实际不会更新，只因为不能是0值
				UpdateTime: nowTime,
			})
		}

		err = db.Object().BatchUpdateColumns(ctx, dummyObjs, []string{"Redundancy", "UpdateTime"})
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
					StorageID:  blk.StorageID,
					CreateTime: nowTime,
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
					CreateTime: nowTime,
				})
			}
		}
		err = db.PinnedObject().BatchTryCreate(ctx, pinneds)
		if err != nil {
			return fmt.Errorf("batch create pinned objects: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.Warnf("batch updating redundancy: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch update redundancy failed")
	}

	return mq.ReplyOK(coormq.RespUpdateObjectRedundancy())
}

func (svc *Service) UpdateObjectInfos(msg *coormq.UpdateObjectInfos) (*coormq.UpdateObjectInfosResp, *mq.CodeMessage) {
	var sucs []cdssdk.ObjectID
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		msg.Updatings = sort2.Sort(msg.Updatings, func(o1, o2 cdsapi.UpdatingObject) int {
			return sort2.Cmp(o1.ObjectID, o2.ObjectID)
		})

		objIDs := make([]cdssdk.ObjectID, len(msg.Updatings))
		for i, obj := range msg.Updatings {
			objIDs[i] = obj.ObjectID
		}

		oldObjs, err := svc.db2.Object().BatchGet(tx, objIDs)
		if err != nil {
			return fmt.Errorf("batch getting objects: %w", err)
		}
		oldObjIDs := make([]cdssdk.ObjectID, len(oldObjs))
		for i, obj := range oldObjs {
			oldObjIDs[i] = obj.ObjectID
		}

		avaiUpdatings, notExistsObjs := pickByObjectIDs(msg.Updatings, oldObjIDs, func(obj cdsapi.UpdatingObject) cdssdk.ObjectID { return obj.ObjectID })
		if len(notExistsObjs) > 0 {
			// TODO 部分对象已经不存在
		}

		newObjs := make([]cdssdk.Object, len(avaiUpdatings))
		for i := range newObjs {
			newObjs[i] = oldObjs[i]
			avaiUpdatings[i].ApplyTo(&newObjs[i])
		}

		err = svc.db2.Object().BatchUpdate(tx, newObjs)
		if err != nil {
			return fmt.Errorf("batch create or update: %w", err)
		}

		sucs = lo.Map(newObjs, func(obj cdssdk.Object, _ int) cdssdk.ObjectID { return obj.ObjectID })
		return nil
	})

	if err != nil {
		logger.Warnf("batch updating objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch update objects failed")
	}

	return mq.ReplyOK(coormq.RespUpdateObjectInfos(sucs))
}

// 根据objIDs从objs中挑选Object。
// len(objs) >= len(objIDs)
func pickByObjectIDs[T any](objs []T, objIDs []cdssdk.ObjectID, getID func(T) cdssdk.ObjectID) (picked []T, notFound []T) {
	objIdx := 0
	idIdx := 0

	for idIdx < len(objIDs) && objIdx < len(objs) {
		if getID(objs[objIdx]) < objIDs[idIdx] {
			notFound = append(notFound, objs[objIdx])
			objIdx++
			continue
		}

		picked = append(picked, objs[objIdx])
		objIdx++
		idIdx++
	}

	return
}

func (svc *Service) MoveObjects(msg *coormq.MoveObjects) (*coormq.MoveObjectsResp, *mq.CodeMessage) {
	var sucs []cdssdk.ObjectID
	var evt []*stgmod.BodyObjectInfoUpdated

	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		msg.Movings = sort2.Sort(msg.Movings, func(o1, o2 cdsapi.MovingObject) int {
			return sort2.Cmp(o1.ObjectID, o2.ObjectID)
		})

		objIDs := make([]cdssdk.ObjectID, len(msg.Movings))
		for i, obj := range msg.Movings {
			objIDs[i] = obj.ObjectID
		}

		oldObjs, err := svc.db2.Object().BatchGet(tx, objIDs)
		if err != nil {
			return fmt.Errorf("batch getting objects: %w", err)
		}
		oldObjIDs := make([]cdssdk.ObjectID, len(oldObjs))
		for i, obj := range oldObjs {
			oldObjIDs[i] = obj.ObjectID
		}

		// 找出仍在数据库的Object
		avaiMovings, notExistsObjs := pickByObjectIDs(msg.Movings, oldObjIDs, func(obj cdsapi.MovingObject) cdssdk.ObjectID { return obj.ObjectID })
		if len(notExistsObjs) > 0 {
			// TODO 部分对象已经不存在
		}

		// 筛选出PackageID变化、Path变化的对象，这两种对象要检测改变后是否有冲突
		var pkgIDChangedObjs []cdssdk.Object
		var pathChangedObjs []cdssdk.Object
		for i := range avaiMovings {
			if avaiMovings[i].PackageID != oldObjs[i].PackageID {
				newObj := oldObjs[i]
				avaiMovings[i].ApplyTo(&newObj)
				pkgIDChangedObjs = append(pkgIDChangedObjs, newObj)
			} else if avaiMovings[i].Path != oldObjs[i].Path {
				newObj := oldObjs[i]
				avaiMovings[i].ApplyTo(&newObj)
				pathChangedObjs = append(pathChangedObjs, newObj)
			}
		}

		var newObjs []cdssdk.Object
		// 对于PackageID发生变化的对象，需要检查目标Package内是否存在同Path的对象
		checkedObjs, err := svc.checkPackageChangedObjects(tx, msg.UserID, pkgIDChangedObjs)
		if err != nil {
			return err
		}
		newObjs = append(newObjs, checkedObjs...)

		// 对于只有Path发生变化的对象，则检查同Package内有没有同Path的对象
		checkedObjs, err = svc.checkPathChangedObjects(tx, msg.UserID, pathChangedObjs)
		if err != nil {
			return err
		}
		newObjs = append(newObjs, checkedObjs...)

		err = svc.db2.Object().BatchUpdate(tx, newObjs)
		if err != nil {
			return fmt.Errorf("batch create or update: %w", err)
		}

		sucs = lo.Map(newObjs, func(obj cdssdk.Object, _ int) cdssdk.ObjectID { return obj.ObjectID })
		evt = lo.Map(newObjs, func(obj cdssdk.Object, _ int) *stgmod.BodyObjectInfoUpdated {
			return &stgmod.BodyObjectInfoUpdated{
				Object: obj,
			}
		})
		return nil
	})
	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "move objects failed")
	}

	for _, e := range evt {
		svc.evtPub.Publish(e)
	}

	return mq.ReplyOK(coormq.RespMoveObjects(sucs))
}

func (svc *Service) checkPackageChangedObjects(tx db2.SQLContext, userID cdssdk.UserID, objs []cdssdk.Object) ([]cdssdk.Object, error) {
	if len(objs) == 0 {
		return nil, nil
	}

	type PackageObjects struct {
		PackageID    cdssdk.PackageID
		ObjectByPath map[string]*cdssdk.Object
	}

	packages := make(map[cdssdk.PackageID]*PackageObjects)
	for _, obj := range objs {
		pkg, ok := packages[obj.PackageID]
		if !ok {
			pkg = &PackageObjects{
				PackageID:    obj.PackageID,
				ObjectByPath: make(map[string]*cdssdk.Object),
			}
			packages[obj.PackageID] = pkg
		}

		if pkg.ObjectByPath[obj.Path] == nil {
			o := obj
			pkg.ObjectByPath[obj.Path] = &o
		} else {
			// TODO 有两个对象移动到同一个路径，有冲突
		}
	}

	var willUpdateObjs []cdssdk.Object
	for _, pkg := range packages {
		_, err := svc.db2.Package().GetUserPackage(tx, userID, pkg.PackageID)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("getting user package by id: %w", err)
		}

		existsObjs, err := svc.db2.Object().BatchGetByPackagePath(tx, pkg.PackageID, lo.Keys(pkg.ObjectByPath))
		if err != nil {
			return nil, fmt.Errorf("batch getting objects by package path: %w", err)
		}

		// 标记冲突的对象
		for _, obj := range existsObjs {
			pkg.ObjectByPath[obj.Path] = nil
			// TODO 目标Package内有冲突的对象
		}

		for _, obj := range pkg.ObjectByPath {
			if obj == nil {
				continue
			}
			willUpdateObjs = append(willUpdateObjs, *obj)
		}
	}

	return willUpdateObjs, nil
}

func (svc *Service) checkPathChangedObjects(tx db2.SQLContext, userID cdssdk.UserID, objs []cdssdk.Object) ([]cdssdk.Object, error) {
	if len(objs) == 0 {
		return nil, nil
	}

	objByPath := make(map[string]*cdssdk.Object)
	for _, obj := range objs {
		if objByPath[obj.Path] == nil {
			o := obj
			objByPath[obj.Path] = &o
		} else {
			// TODO 有两个对象移动到同一个路径，有冲突
		}

	}

	_, err := svc.db2.Package().GetUserPackage(tx, userID, objs[0].PackageID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting user package by id: %w", err)
	}

	existsObjs, err := svc.db2.Object().BatchGetByPackagePath(tx, objs[0].PackageID, lo.Map(objs, func(obj cdssdk.Object, idx int) string { return obj.Path }))
	if err != nil {
		return nil, fmt.Errorf("batch getting objects by package path: %w", err)
	}

	// 不支持两个对象交换位置的情况，因为数据库不支持
	for _, obj := range existsObjs {
		objByPath[obj.Path] = nil
	}

	var willMoveObjs []cdssdk.Object
	for _, obj := range objByPath {
		if obj == nil {
			continue
		}
		willMoveObjs = append(willMoveObjs, *obj)
	}

	return willMoveObjs, nil
}

func (svc *Service) DeleteObjects(msg *coormq.DeleteObjects) (*coormq.DeleteObjectsResp, *mq.CodeMessage) {
	var sucs []cdssdk.ObjectID
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		avaiIDs, err := svc.db2.Object().BatchTestObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch testing object id: %w", err)
		}
		sucs = lo.Keys(avaiIDs)

		err = svc.db2.Object().BatchDelete(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch deleting objects: %w", err)
		}

		err = svc.db2.ObjectBlock().BatchDeleteByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch deleting object blocks: %w", err)
		}

		err = svc.db2.PinnedObject().BatchDeleteByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch deleting pinned objects: %w", err)
		}

		err = svc.db2.ObjectAccessStat().BatchDeleteByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch deleting object access stats: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.Warnf("batch deleting objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch delete objects failed")
	}

	for _, objID := range sucs {
		svc.evtPub.Publish(&stgmod.BodyObjectDeleted{
			ObjectID: objID,
		})
	}

	return mq.ReplyOK(coormq.RespDeleteObjects(sucs))
}

func (svc *Service) CloneObjects(msg *coormq.CloneObjects) (*coormq.CloneObjectsResp, *mq.CodeMessage) {
	type CloningObject struct {
		Cloning  cdsapi.CloningObject
		OrgIndex int
	}
	type PackageClonings struct {
		PackageID cdssdk.PackageID
		Clonings  map[string]CloningObject
	}

	var evt []*stgmod.BodyNewOrUpdateObject

	// TODO 要检查用户是否有Object、Package的权限
	clonings := make(map[cdssdk.PackageID]*PackageClonings)
	for i, cloning := range msg.Clonings {
		pkg, ok := clonings[cloning.NewPackageID]
		if !ok {
			pkg = &PackageClonings{
				PackageID: cloning.NewPackageID,
				Clonings:  make(map[string]CloningObject),
			}
			clonings[cloning.NewPackageID] = pkg
		}
		pkg.Clonings[cloning.NewPath] = CloningObject{
			Cloning:  cloning,
			OrgIndex: i,
		}
	}

	ret := make([]*cdssdk.Object, len(msg.Clonings))
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		// 剔除掉新路径已经存在的对象
		for _, pkg := range clonings {
			exists, err := svc.db2.Object().BatchGetByPackagePath(tx, pkg.PackageID, lo.Keys(pkg.Clonings))
			if err != nil {
				return fmt.Errorf("batch getting objects by package path: %w", err)
			}

			for _, obj := range exists {
				delete(pkg.Clonings, obj.Path)
			}
		}

		// 删除目的Package不存在的对象
		newPkg, err := svc.db2.Package().BatchTestPackageID(tx, lo.Keys(clonings))
		if err != nil {
			return fmt.Errorf("batch testing package id: %w", err)
		}
		for _, pkg := range clonings {
			if !newPkg[pkg.PackageID] {
				delete(clonings, pkg.PackageID)
			}
		}

		var avaiClonings []CloningObject
		var avaiObjIDs []cdssdk.ObjectID
		for _, pkg := range clonings {
			for _, cloning := range pkg.Clonings {
				avaiClonings = append(avaiClonings, cloning)
				avaiObjIDs = append(avaiObjIDs, cloning.Cloning.ObjectID)
			}
		}

		avaiDetails, err := svc.db2.Object().BatchGetDetails(tx, avaiObjIDs)
		if err != nil {
			return fmt.Errorf("batch getting object details: %w", err)
		}

		avaiDetailsMap := make(map[cdssdk.ObjectID]stgmod.ObjectDetail)
		for _, detail := range avaiDetails {
			avaiDetailsMap[detail.Object.ObjectID] = detail
		}

		oldAvaiClonings := avaiClonings
		avaiClonings = nil

		var newObjs []cdssdk.Object
		for _, cloning := range oldAvaiClonings {
			// 进一步剔除原始对象不存在的情况
			detail, ok := avaiDetailsMap[cloning.Cloning.ObjectID]
			if !ok {
				continue
			}

			avaiClonings = append(avaiClonings, cloning)

			newObj := detail.Object
			newObj.ObjectID = 0
			newObj.Path = cloning.Cloning.NewPath
			newObj.PackageID = cloning.Cloning.NewPackageID
			newObjs = append(newObjs, newObj)
		}

		// 先创建出新对象
		err = svc.db2.Object().BatchCreate(tx, &newObjs)
		if err != nil {
			return fmt.Errorf("batch creating objects: %w", err)
		}

		// 创建了新对象就能拿到新对象ID，再创建新对象块
		var newBlks []stgmod.ObjectBlock
		for i, cloning := range avaiClonings {
			oldBlks := avaiDetailsMap[cloning.Cloning.ObjectID].Blocks
			for _, blk := range oldBlks {
				newBlk := blk
				newBlk.ObjectID = newObjs[i].ObjectID
				newBlks = append(newBlks, newBlk)
			}
		}

		err = svc.db2.ObjectBlock().BatchCreate(tx, newBlks)
		if err != nil {
			return fmt.Errorf("batch creating object blocks: %w", err)
		}

		for i, cloning := range avaiClonings {
			ret[cloning.OrgIndex] = &newObjs[i]
		}

		for i, cloning := range avaiClonings {
			var evtBlks []stgmod.BlockDistributionObjectInfo
			blkType := getBlockTypeFromRed(newObjs[i].Redundancy)

			oldBlks := avaiDetailsMap[cloning.Cloning.ObjectID].Blocks
			for _, blk := range oldBlks {
				evtBlks = append(evtBlks, stgmod.BlockDistributionObjectInfo{
					BlockType: blkType,
					Index:     blk.Index,
					StorageID: blk.StorageID,
				})
			}

			evt = append(evt, &stgmod.BodyNewOrUpdateObject{
				Info:              newObjs[i],
				BlockDistribution: evtBlks,
			})
		}
		return nil
	})

	if err != nil {
		logger.Warnf("cloning objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	for _, e := range evt {
		svc.evtPub.Publish(e)
	}

	return mq.ReplyOK(coormq.RespCloneObjects(ret))
}
