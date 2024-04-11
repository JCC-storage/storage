package mq

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// GetPackage 通过PackageID获取包信息
// 参数:
// - msg: 包含需要获取的PackageID的请求消息
// 返回值:
// - *coormq.GetPackageResp: 获取包信息成功的响应
// - *mq.CodeMessage: 错误时返回的错误信息
func (svc *Service) GetPackage(msg *coormq.GetPackage) (*coormq.GetPackageResp, *mq.CodeMessage) {
	// 通过ID从数据库获取包信息
	pkg, err := svc.db.Package().GetByID(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		// 记录日志并返回错误信息
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package failed")
	}

	// 返回成功响应
	return mq.ReplyOK(coormq.NewGetPackageResp(pkg))
}

// CreatePackage 创建一个新的包
// 参数:
// - msg: 包含创建包所需信息的请求消息
// 返回值:
// - *coormq.CreatePackageResp: 创建包成功的响应
// - *mq.CodeMessage: 错误时返回的错误信息
func (svc *Service) CreatePackage(msg *coormq.CreatePackage) (*coormq.CreatePackageResp, *mq.CodeMessage) {
	var pkgID cdssdk.PackageID
	// 在事务中执行创建包的操作
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		var err error

		// 检查桶是否可用
		isAvai, _ := svc.db.Bucket().IsAvailable(tx, msg.BucketID, msg.UserID)
		if !isAvai {
			return fmt.Errorf("bucket is not avaiable to the user")
		}

		// 创建包
		pkgID, err = svc.db.Package().Create(tx, msg.BucketID, msg.Name)
		if err != nil {
			return fmt.Errorf("creating package: %w", err)
		}

		return nil
	})
	if err != nil {
		// 记录日志并返回错误信息
		logger.WithField("BucketID", msg.BucketID).
			WithField("Name", msg.Name).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "creating package failed")
	}

	// 返回成功响应
	return mq.ReplyOK(coormq.NewCreatePackageResp(pkgID))
}

// UpdatePackage 更新包的信息
// 参数:
// - msg: 包含更新包所需信息的请求消息
// 返回值:
// - *coormq.UpdatePackageResp: 更新包成功的响应
// - *mq.CodeMessage: 错误时返回的错误信息
func (svc *Service) UpdatePackage(msg *coormq.UpdatePackage) (*coormq.UpdatePackageResp, *mq.CodeMessage) {
	// 在事务中执行更新包的操作
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		// 验证包是否存在
		_, err := svc.db.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		// 删除对象
		if len(msg.Deletes) > 0 {
			if err := svc.db.Object().BatchDelete(tx, msg.Deletes); err != nil {
				return fmt.Errorf("deleting objects: %w", err)
			}
		}

		// 添加对象
		if len(msg.Adds) > 0 {
			if _, err := svc.db.Object().BatchAdd(tx, msg.PackageID, msg.Adds); err != nil {
				return fmt.Errorf("adding objects: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		// 记录日志并返回错误信息
		logger.WithField("PackageID", msg.PackageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "update package failed")
	}

	// 返回成功响应
	return mq.ReplyOK(coormq.NewUpdatePackageResp())
}

// DeletePackage 删除一个包
// 参数:
// - msg: 包含删除包所需信息的请求消息
// 返回值:
// - *coormq.DeletePackageResp: 删除包成功的响应
// - *mq.CodeMessage: 错误时返回的错误信息
func (svc *Service) DeletePackage(msg *coormq.DeletePackage) (*coormq.DeletePackageResp, *mq.CodeMessage) {
	// 在事务中执行删除包的操作
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		// 验证包是否可用
		isAvai, _ := svc.db.Package().IsAvailable(tx, msg.UserID, msg.PackageID)
		if !isAvai {
			return fmt.Errorf("package is not available to the user")
		}

		// 软删除包
		err := svc.db.Package().SoftDelete(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("soft delete package: %w", err)
		}

		// 删除未使用的包
		err = svc.db.Package().DeleteUnused(tx, msg.PackageID)
		if err != nil {
			logger.WithField("UserID", msg.UserID).
				WithField("PackageID", msg.PackageID).
				Warnf("deleting unused package: %w", err.Error())
		}

		return nil
	})
	if err != nil {
		// 记录日志并返回错误信息
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "delete package failed")
	}

	// 返回成功响应
	return mq.ReplyOK(coormq.NewDeletePackageResp())
}

// GetPackageCachedNodes 获取缓存了指定package的节点信息
// 参数:
// - msg: 包含packageID和用户ID的信息请求
// 返回值:
// - *coormq.GetPackageCachedNodesResp: 包含缓存了package数据的节点信息列表
// - *mq.CodeMessage: 错误信息，如果操作失败
func (svc *Service) GetPackageCachedNodes(msg *coormq.GetPackageCachedNodes) (*coormq.GetPackageCachedNodesResp, *mq.CodeMessage) {
	// 检查package是否可用
	isAva, err := svc.db.Package().IsAvailable(svc.db.SQLCtx(), msg.UserID, msg.PackageID)
	if err != nil {
		// 记录检查package可用性失败的日志
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("check package available failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "check package available failed")
	}
	if !isAva {
		// 记录package不可用的日志
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("package is not available to the user")
		return nil, mq.Failed(errorcode.OperationFailed, "package is not available to the user")
	}

	// 获取package中的对象详情，用于后续统计节点缓存信息
	objDetails, err := svc.db.Object().GetPackageObjectDetails(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		// 记录获取package对象详情失败的日志
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package block details: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package block details failed")
	}

	// 统计各节点缓存的文件信息
	var packageSize int64
	nodeInfoMap := make(map[cdssdk.NodeID]*cdssdk.NodePackageCachingInfo)
	for _, obj := range objDetails {
		for _, block := range obj.Blocks {
			// 更新或创建节点缓存信息
			info, ok := nodeInfoMap[block.NodeID]
			if !ok {
				info = &cdssdk.NodePackageCachingInfo{
					NodeID: block.NodeID,
				}
				nodeInfoMap[block.NodeID] = info
			}

			// 更新节点的文件大小和对象计数
			info.FileSize += obj.Object.Size
			info.ObjectCount++
		}
	}

	// 整理节点缓存信息，并按节点ID排序
	var nodeInfos []cdssdk.NodePackageCachingInfo
	for _, nodeInfo := range nodeInfoMap {
		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	sort.Slice(nodeInfos, func(i, j int) bool {
		return nodeInfos[i].NodeID < nodeInfos[j].NodeID
	})
	// 返回成功响应，包含节点缓存信息
	return mq.ReplyOK(coormq.NewGetPackageCachedNodesResp(nodeInfos, packageSize))
}

// GetPackageLoadedNodes 获取加载了指定package的节点ID列表
// 参数:
// - msg: 包含packageID的信息请求
// 返回值:
// - *coormq.GetPackageLoadedNodesResp: 包含加载了package的节点ID列表
// - *mq.CodeMessage: 错误信息，如果操作失败
func (svc *Service) GetPackageLoadedNodes(msg *coormq.GetPackageLoadedNodes) (*coormq.GetPackageLoadedNodesResp, *mq.CodeMessage) {
	// 根据packageID查找相关的存储信息
	storages, err := svc.db.StoragePackage().FindPackageStorages(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		// 记录查找存储信息失败的日志
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get storages by packageID failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get storages by packageID failed")
	}

	// 去重，获取唯一节点ID列表
	uniqueNodeIDs := make(map[cdssdk.NodeID]bool)
	var nodeIDs []cdssdk.NodeID
	for _, stg := range storages {
		if !uniqueNodeIDs[stg.NodeID] {
			uniqueNodeIDs[stg.NodeID] = true
			nodeIDs = append(nodeIDs, stg.NodeID)
		}
	}

	// 返回成功响应，包含节点ID列表
	return mq.ReplyOK(coormq.NewGetPackageLoadedNodesResp(nodeIDs))
}

// GetPackageLoadLogDetails 获取指定package的加载日志详情
// 参数:
// - msg: 包含packageID的信息请求
// 返回值:
// - *coormq.GetPackageLoadLogDetailsResp: 包含package加载日志的详细信息列表
// - *mq.CodeMessage: 错误信息，如果操作失败
func (svc *Service) GetPackageLoadLogDetails(msg *coormq.GetPackageLoadLogDetails) (*coormq.GetPackageLoadLogDetailsResp, *mq.CodeMessage) {
	var logs []coormq.PackageLoadLogDetail
	// 根据packageID获取加载日志
	rawLogs, err := svc.db.StoragePackageLog().GetByPackageID(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		// 记录获取加载日志失败的日志
		logger.WithField("PackageID", msg.PackageID).
			Warnf("getting storage package log: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get storage package log failed")
	}

	// 通过存储ID获取存储信息，用于填充日志详情
	stgs := make(map[cdssdk.StorageID]model.Storage)

	for _, raw := range rawLogs {
		stg, ok := stgs[raw.StorageID]
		if !ok {
			stg, err = svc.db.Storage().GetByID(svc.db.SQLCtx(), raw.StorageID)
			if err != nil {
				// 记录获取存储信息失败的日志
				logger.WithField("PackageID", msg.PackageID).
					Warnf("getting storage: %s", err.Error())
				return nil, mq.Failed(errorcode.OperationFailed, "get storage failed")
			}

			stgs[raw.StorageID] = stg
		}

		// 填充日志详情
		logs = append(logs, coormq.PackageLoadLogDetail{
			Storage:    stg,
			UserID:     raw.UserID,
			CreateTime: raw.CreateTime,
		})
	}

	// 返回成功响应，包含package加载日志详情
	return mq.ReplyOK(coormq.RespGetPackageLoadLogDetails(logs))
}
