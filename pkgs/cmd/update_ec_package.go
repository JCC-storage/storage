package cmd

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/utils/serder"
	mysort "gitlink.org.cn/cloudream/common/utils/sort"

	"gitlink.org.cn/cloudream/storage-common/globals"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type UpdateECPackage struct {
	userID     int64
	packageID  int64
	objectIter iterator.UploadingObjectIterator
}

type UpdateECPackageResult struct {
	ObjectResults []ECObjectUploadResult
}

func NewUpdateECPackage(userID int64, packageID int64, objIter iterator.UploadingObjectIterator) *UpdateECPackage {
	return &UpdateECPackage{
		userID:     userID,
		packageID:  packageID,
		objectIter: objIter,
	}
}

func (t *UpdateECPackage) Execute(ctx *UpdateECPackageContext) (*UpdateECPackageResult, error) {
	defer t.objectIter.Close()

	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	/*
		TODO2
		reqBlder := reqbuilder.NewBuilder()

		// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
		if t.uploadConfig.LocalNodeID != nil {
			reqBlder.IPFS().CreateAnyRep(*t.uploadConfig.LocalNodeID)
		}

		// TODO2
		mutex, err := reqBlder.
			Metadata().
			// 用于判断用户是否有对象的权限
			UserBucket().ReadAny().
			// 用于读取、修改对象信息
			Object().WriteOne(t.objectID).
			// 用于更新Rep配置
			ObjectRep().WriteOne(t.objectID).
			// 用于查询可用的上传节点
			Node().ReadAny().
			// 用于创建Cache记录
			Cache().CreateAny().
			// 用于修改Move此Object的记录的状态
			StorageObject().WriteAny().
			MutexLock(ctx.DistLock())
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
	*/

	getPkgResp, err := coorCli.GetPackage(coormq.NewGetPackage(t.userID, t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package: %w", err)
	}

	getUserNodesResp, err := coorCli.GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return nil, fmt.Errorf("getting user nodes: %w", err)
	}

	findCliLocResp, err := coorCli.FindClientLocation(coormq.NewFindClientLocation(globals.Local.ExternalIP))
	if err != nil {
		return nil, fmt.Errorf("finding client location: %w", err)
	}

	nodeInfos := lo.Map(getUserNodesResp.Nodes, func(node model.Node, index int) UploadNodeInfo {
		return UploadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == findCliLocResp.Location.LocationID,
		}
	})

	var ecRed models.ECRedundancyInfo
	if err := serder.AnyToAny(getPkgResp.Package.Redundancy.Info, &ecRed); err != nil {
		return nil, fmt.Errorf("get ec redundancy info: %w", err)
	}

	getECResp, err := coorCli.GetECConfig(coormq.NewGetECConfig(ecRed.ECName))
	if err != nil {
		return nil, fmt.Errorf("getting ec: %w", err)
	}

	/*
		TODO2
		// 防止上传的副本被清除
		mutex2, err := reqbuilder.NewBuilder().
			IPFS().CreateAnyRep(uploadNode.Node.NodeID).
			MutexLock(ctx.DistLock())
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex2.Unlock()
	*/

	rets, err := uploadAndUpdateECPackage(ctx, t.packageID, t.objectIter, nodeInfos, getECResp.Config)
	if err != nil {
		return nil, err
	}

	return &UpdateECPackageResult{
		ObjectResults: rets,
	}, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (t *UpdateECPackage) chooseUploadNode(nodes []UpdateNodeInfo) UpdateNodeInfo {
	mysort.Sort(nodes, func(left, right UpdateNodeInfo) int {
		v := -mysort.CmpBool(left.HasOldObject, right.HasOldObject)
		if v != 0 {
			return v
		}

		return -mysort.CmpBool(left.IsSameLocation, right.IsSameLocation)
	})

	return nodes[0]
}
