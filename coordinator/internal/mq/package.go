package mq

import (
	"database/sql"
	"fmt"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	"sort"

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

		if err == sql.ErrNoRows {
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

		pkgID, err := svc.db2.Package().Create(tx, msg.BucketID, msg.Name)
		if err != nil {
			return fmt.Errorf("creating package: %w", err)
		}

		pkg, err = svc.db2.Package().GetByID(tx, pkgID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("BucketID", msg.BucketID).
			WithField("Name", msg.Name).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "creating package failed")
	}

	return mq.ReplyOK(coormq.NewCreatePackageResp(pkg))
}

func (svc *Service) UpdatePackage(msg *coormq.UpdatePackage) (*coormq.UpdatePackageResp, *mq.CodeMessage) {
	var added []cdssdk.Object
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		_, err := svc.db2.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		// 先执行删除操作
		if len(msg.Deletes) > 0 {
			if err := svc.db2.Object().BatchDelete(tx, msg.Deletes); err != nil {
				return fmt.Errorf("deleting objects: %w", err)
			}
		}

		// 再执行添加操作
		if len(msg.Adds) > 0 {
			ad, err := svc.db2.Object().BatchAdd(tx, msg.PackageID, msg.Adds)
			if err != nil {
				return fmt.Errorf("adding objects: %w", err)
			}
			added = ad
		}

		return nil
	})
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "update package failed")
	}

	return mq.ReplyOK(coormq.NewUpdatePackageResp(added))
}

func (svc *Service) DeletePackage(msg *coormq.DeletePackage) (*coormq.DeletePackageResp, *mq.CodeMessage) {
	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		isAvai, _ := svc.db2.Package().IsAvailable(tx, msg.UserID, msg.PackageID)
		if !isAvai {
			return fmt.Errorf("package is not available to the user")
		}

		err := svc.db2.Package().SoftDelete(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("soft delete package: %w", err)
		}

		err = svc.db2.Package().DeleteUnused(tx, msg.PackageID)
		if err != nil {
			logger.WithField("UserID", msg.UserID).
				WithField("PackageID", msg.PackageID).
				Warnf("deleting unused package: %w", err.Error())
		}

		err = svc.db2.PackageAccessStat().DeleteByPackageID(tx, msg.PackageID)
		if err != nil {
			logger.WithField("UserID", msg.UserID).
				WithField("PackageID", msg.PackageID).
				Warnf("deleting package access stat: %w", err.Error())
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "delete package failed")
	}

	return mq.ReplyOK(coormq.NewDeletePackageResp())
}

func (svc *Service) GetPackageCachedNodes(msg *coormq.GetPackageCachedNodes) (*coormq.GetPackageCachedNodesResp, *mq.CodeMessage) {
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
	nodeInfoMap := make(map[cdssdk.NodeID]*cdssdk.NodePackageCachingInfo)
	for _, obj := range objDetails {
		// 只要存了文件的一个块，就认为此节点存了整个文件
		for _, block := range obj.Blocks {
			info, ok := nodeInfoMap[block.NodeID]
			if !ok {
				info = &cdssdk.NodePackageCachingInfo{
					NodeID: block.NodeID,
				}
				nodeInfoMap[block.NodeID] = info

			}

			info.FileSize += obj.Object.Size
			info.ObjectCount++
		}
	}

	var nodeInfos []cdssdk.NodePackageCachingInfo
	for _, nodeInfo := range nodeInfoMap {
		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	sort.Slice(nodeInfos, func(i, j int) bool {
		return nodeInfos[i].NodeID < nodeInfos[j].NodeID
	})
	return mq.ReplyOK(coormq.NewGetPackageCachedNodesResp(nodeInfos, packageSize))
}

func (svc *Service) GetPackageLoadedNodes(msg *coormq.GetPackageLoadedNodes) (*coormq.GetPackageLoadedNodesResp, *mq.CodeMessage) {
	storages, err := svc.db2.StoragePackage().FindPackageStorages(svc.db2.DefCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get storages by packageID failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get storages by packageID failed")
	}

	uniqueNodeIDs := make(map[cdssdk.NodeID]bool)
	var nodeIDs []cdssdk.NodeID
	for _, stg := range storages {
		if !uniqueNodeIDs[stg.NodeID] {
			uniqueNodeIDs[stg.NodeID] = true
			nodeIDs = append(nodeIDs, stg.NodeID)
		}
	}

	return mq.ReplyOK(coormq.NewGetPackageLoadedNodesResp(nodeIDs))
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
