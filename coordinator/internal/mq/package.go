package mq

import (
	"errors"
	"fmt"
	"sort"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	"gorm.io/gorm"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetPackage(msg *coormq.GetPackage) (*coormq.GetPackageResp, *mq.CodeMessage) {
	pkg, err := svc.db2.Package().GetByID(svc.db2.DefCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package: %s", err.Error())

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, mq.Failed(errorcode.DataNotFound, "package not found")
		}

		return nil, mq.Failed(errorcode.OperationFailed, "get package failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageResp(pkg))
}

func (svc *Service) GetPackageByName(msg *coormq.GetPackageByName) (*coormq.GetPackageByNameResp, *mq.CodeMessage) {
	pkg, err := svc.db2.Package().GetUserPackageByName(svc.db2.DefCtx(), msg.UserID, msg.BucketName, msg.PackageName)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketName", msg.BucketName).
			WithField("PackageName", msg.PackageName).
			Warnf("get package by name: %s", err.Error())

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, mq.Failed(errorcode.DataNotFound, "package not found")
		}

		return nil, mq.Failed(errorcode.OperationFailed, "get package by name failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageByNameResp(pkg))
}

func (svc *Service) CreatePackage(msg *coormq.CreatePackage) (*coormq.CreatePackageResp, *mq.CodeMessage) {
	var pkg cdssdk.Package
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		var err error

		isAvai, _ := svc.db2.Bucket().IsAvailable(tx, msg.BucketID, msg.UserID)
		if !isAvai {
			return fmt.Errorf("bucket is not avaiable to the user")
		}

		pkg, err = svc.db2.Package().Create(tx, msg.BucketID, msg.Name)
		if err != nil {
			return fmt.Errorf("creating package: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("BucketID", msg.BucketID).
			WithField("Name", msg.Name).
			Warn(err.Error())

		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return nil, mq.Failed(errorcode.DataExists, "package already exists")
		}

		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	svc.evtPub.Publish(&stgmod.BodyNewPackage{
		Info: pkg,
	})

	return mq.ReplyOK(coormq.NewCreatePackageResp(pkg))
}

func (svc *Service) UpdatePackage(msg *coormq.UpdatePackage) (*coormq.UpdatePackageResp, *mq.CodeMessage) {
	var added []cdssdk.Object
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		_, err := svc.db2.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		ad, err := svc.db2.Object().BatchAdd(tx, msg.PackageID, msg.Adds)
		if err != nil {
			return fmt.Errorf("adding objects: %w", err)
		}
		added = ad

		return nil
	})
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "update package failed")
	}

	addedMp := make(map[string]cdssdk.Object)
	for _, obj := range added {
		addedMp[obj.Path] = obj
	}

	for _, add := range msg.Adds {
		var blks []stgmod.BlockDistributionObjectInfo
		for _, stgID := range add.StorageIDs {
			blks = append(blks, stgmod.BlockDistributionObjectInfo{
				BlockType: stgmod.BlockTypeRaw,
				StorageID: stgID,
			})
		}

		svc.evtPub.Publish(&stgmod.BodyNewOrUpdateObject{
			Info:              addedMp[add.Path],
			BlockDistribution: blks,
		})
	}

	return mq.ReplyOK(coormq.NewUpdatePackageResp(added))
}

func (svc *Service) DeletePackage(msg *coormq.DeletePackage) (*coormq.DeletePackageResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		isAvai, _ := svc.db2.Package().IsAvailable(tx, msg.UserID, msg.PackageID)
		if !isAvai {
			return fmt.Errorf("package is not available to the user")
		}

		err := svc.db2.Package().DeleteComplete(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("deleting package: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "delete package failed")
	}

	svc.evtPub.Publish(&stgmod.BodyPackageDeleted{
		PackageID: msg.PackageID,
	})

	return mq.ReplyOK(coormq.NewDeletePackageResp())
}

func (svc *Service) ClonePackage(msg *coormq.ClonePackage) (*coormq.ClonePackageResp, *mq.CodeMessage) {
	var pkg cdssdk.Package
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		var err error

		isAvai, _ := svc.db2.Bucket().IsAvailable(tx, msg.BucketID, msg.UserID)
		if !isAvai {
			return fmt.Errorf("bucket is not avaiable to the user")
		}

		pkg, err = svc.db2.Package().Create(tx, msg.BucketID, msg.Name)
		if err != nil {
			return fmt.Errorf("creating package: %w", err)
		}

		objs, err := svc.db2.Object().GetPackageObjects(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package objects: %w", err)
		}

		objBlks, err := svc.db2.ObjectBlock().GetInPackageID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting object blocks: %w", err)
		}

		clonedObjs := make([]cdssdk.Object, len(objs))
		for i, obj := range objs {
			clonedObjs[i] = obj
			clonedObjs[i].ObjectID = 0
			clonedObjs[i].PackageID = pkg.PackageID
		}

		err = svc.db2.Object().BatchCreate(tx, &clonedObjs)
		if err != nil {
			return fmt.Errorf("batch creating objects: %w", err)
		}

		oldToNew := make(map[cdssdk.ObjectID]cdssdk.ObjectID)
		for i, obj := range clonedObjs {
			oldToNew[objs[i].ObjectID] = obj.ObjectID
		}

		clonedBlks := make([]stgmod.ObjectBlock, len(objBlks))
		for i, blk := range objBlks {
			clonedBlks[i] = blk
			clonedBlks[i].ObjectID = oldToNew[blk.ObjectID]
		}

		err = svc.db2.ObjectBlock().BatchCreate(tx, clonedBlks)
		if err != nil {
			return fmt.Errorf("batch creating object blocks: %w", err)
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return nil, mq.Failed(errorcode.DataExists, "package already exists")
		}

		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	svc.evtPub.Publish(&stgmod.BodyPackageCloned{
		SourcePackageID: msg.PackageID,
		NewPackage:      pkg,
	})

	return mq.ReplyOK(coormq.RespClonePackage(pkg))
}

func (svc *Service) GetPackageCachedStorages(msg *coormq.GetPackageCachedStorages) (*coormq.GetPackageCachedStoragesResp, *mq.CodeMessage) {
	isAva, err := svc.db2.Package().IsAvailable(svc.db2.DefCtx(), msg.UserID, msg.PackageID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("check package available failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "check package available failed")
	}
	if !isAva {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("package is not available to the user")
		return nil, mq.Failed(errorcode.OperationFailed, "package is not available to the user")
	}

	// 这个函数只是统计哪些节点缓存了Package中的数据，不需要多么精确，所以可以不用事务
	objDetails, err := svc.db2.Object().GetPackageObjectDetails(svc.db2.DefCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package block details: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package block details failed")
	}

	var packageSize int64
	stgInfoMap := make(map[cdssdk.StorageID]*cdssdk.StoragePackageCachingInfo)
	for _, obj := range objDetails {
		// 只要存了文件的一个块，就认为此节点存了整个文件
		for _, block := range obj.Blocks {
			info, ok := stgInfoMap[block.StorageID]
			if !ok {
				info = &cdssdk.StoragePackageCachingInfo{
					StorageID: block.StorageID,
				}
				stgInfoMap[block.StorageID] = info

			}

			info.FileSize += obj.Object.Size
			info.ObjectCount++
		}
	}

	var stgInfos []cdssdk.StoragePackageCachingInfo
	for _, stgInfo := range stgInfoMap {
		stgInfos = append(stgInfos, *stgInfo)
	}

	sort.Slice(stgInfos, func(i, j int) bool {
		return stgInfos[i].StorageID < stgInfos[j].StorageID
	})
	return mq.ReplyOK(coormq.ReqGetPackageCachedStoragesResp(stgInfos, packageSize))
}

func (svc *Service) AddAccessStat(msg *coormq.AddAccessStat) {
	pkgIDs := make([]cdssdk.PackageID, len(msg.Entries))
	objIDs := make([]cdssdk.ObjectID, len(msg.Entries))
	for i, e := range msg.Entries {
		pkgIDs[i] = e.PackageID
		objIDs[i] = e.ObjectID
	}

	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		avaiPkgIDs, err := svc.db2.Package().BatchTestPackageID(tx, pkgIDs)
		if err != nil {
			return fmt.Errorf("batch test package id: %w", err)
		}

		avaiObjIDs, err := svc.db2.Object().BatchTestObjectID(tx, objIDs)
		if err != nil {
			return fmt.Errorf("batch test object id: %w", err)
		}

		var willAdds []coormq.AddAccessStatEntry
		for _, e := range msg.Entries {
			if avaiPkgIDs[e.PackageID] && avaiObjIDs[e.ObjectID] {
				willAdds = append(willAdds, e)
			}
		}

		if len(willAdds) > 0 {
			err := svc.db2.PackageAccessStat().BatchAddCounter(tx, willAdds)
			if err != nil {
				return fmt.Errorf("batch add package access stat counter: %w", err)
			}

			err = svc.db2.ObjectAccessStat().BatchAddCounter(tx, willAdds)
			if err != nil {
				return fmt.Errorf("batch add object access stat counter: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		logger.Warn(err.Error())
	}
}
