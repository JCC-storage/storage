package services

import (
	"context"
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/plans"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// ObjectService 定义了对象服务，负责管理对象的上传、下载等操作。
type ObjectService struct {
	*Service
}

// ObjectSvc 返回一个ObjectService的实例。
func (svc *Service) ObjectSvc() *ObjectService {
	return &ObjectService{Service: svc}
}

func (svc *ObjectService) GetByPath(userID cdssdk.UserID, pkgID cdssdk.PackageID, path string, isPrefix bool, noRecursive bool) ([]string, []cdssdk.Object, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	listResp, err := coorCli.GetObjectsByPath(coormq.ReqGetObjectsByPath(userID, pkgID, path, isPrefix, noRecursive))
	if err != nil {
		return nil, nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return listResp.CommonPrefixes, listResp.Objects, nil
}

func (svc *ObjectService) GetByIDs(userID cdssdk.UserID, objectIDs []cdssdk.ObjectID) ([]*cdssdk.Object, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	listResp, err := coorCli.GetObjects(coormq.ReqGetObjects(userID, objectIDs))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return listResp.Objects, nil
}

func (svc *ObjectService) UpdateInfo(userID cdssdk.UserID, updatings []cdsapi.UpdatingObject) ([]cdssdk.ObjectID, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.UpdateObjectInfos(coormq.ReqUpdateObjectInfos(userID, updatings))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	// TODO 考虑产生Update事件

	return resp.Successes, nil
}

func (svc *ObjectService) Move(userID cdssdk.UserID, movings []cdsapi.MovingObject) ([]cdssdk.ObjectID, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.MoveObjects(coormq.ReqMoveObjects(userID, movings))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return resp.Successes, nil
}

func (svc *ObjectService) Download(userID cdssdk.UserID, req downloader.DownloadReqeust) (*downloader.Downloading, error) {
	// TODO 检查用户ID
	iter := svc.Downloader.DownloadObjects([]downloader.DownloadReqeust{req})

	// 初始化下载过程
	downloading, err := iter.MoveNext()
	if downloading == nil {
		return nil, fmt.Errorf("object %v not found", req.ObjectID)
	}
	if err != nil {
		return nil, err
	}

	return downloading, nil
}

func (svc *ObjectService) Delete(userID cdssdk.UserID, objectIDs []cdssdk.ObjectID) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	_, err = coorCli.DeleteObjects(coormq.ReqDeleteObjects(userID, objectIDs))
	if err != nil {
		return fmt.Errorf("requsting to coodinator: %w", err)
	}

	return nil
}

func (svc *ObjectService) Clone(userID cdssdk.UserID, clonings []cdsapi.CloningObject) ([]*cdssdk.Object, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.CloneObjects(coormq.ReqCloneObjects(userID, clonings))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return resp.Objects, nil
}

// GetPackageObjects 获取包中的对象列表。
// userID: 用户ID。
// packageID: 包ID。
// 返回值: 对象列表和错误信息。
func (svc *ObjectService) GetPackageObjects(userID cdssdk.UserID, packageID cdssdk.PackageID) ([]model.Object, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire() // 获取协调器客户端
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli) // 释放协调器客户端资源

	getResp, err := coorCli.GetPackageObjects(coormq.ReqGetPackageObjects(userID, packageID)) // 请求协调器获取套餐对象
	if err != nil {
		return nil, fmt.Errorf("requesting to coordinator: %w", err)
	}

	return getResp.Objects, nil
}

func (svc *ObjectService) GetObjectDetail(objectID cdssdk.ObjectID) (*stgmod.ObjectDetail, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetObjectDetails(coormq.ReqGetObjectDetails([]cdssdk.ObjectID{objectID}))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return getResp.Objects[0], nil
}

func (svc *ObjectService) NewMultipartUploadObject(userID cdssdk.UserID, pkgID cdssdk.PackageID, path string) (cdssdk.Object, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.Object{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.NewMultipartUploadObject(coormq.ReqNewMultipartUploadObject(userID, pkgID, path))
	if err != nil {
		return cdssdk.Object{}, err
	}

	return resp.Object, nil
}

func (svc *ObjectService) CompleteMultipartUpload(userID cdssdk.UserID, objectID cdssdk.ObjectID, indexes []int) (cdssdk.Object, error) {
	if len(indexes) == 0 {
		return cdssdk.Object{}, fmt.Errorf("no block indexes specified")
	}

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.Object{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	details, err := coorCli.GetObjectDetails(coormq.ReqGetObjectDetails([]cdssdk.ObjectID{objectID}))
	if err != nil {
		return cdssdk.Object{}, err
	}

	if details.Objects[0] == nil {
		return cdssdk.Object{}, fmt.Errorf("object %v not found", objectID)
	}

	objDe := details.Objects[0]

	_, ok := objDe.Object.Redundancy.(*cdssdk.MultipartUploadRedundancy)
	if !ok {
		return cdssdk.Object{}, fmt.Errorf("object %v is not a multipart upload", objectID)
	}

	if len(objDe.Blocks) == 0 {
		return cdssdk.Object{}, fmt.Errorf("object %v has no blocks", objectID)
	}

	objBlkMap := make(map[int]stgmod.ObjectBlock)
	for _, blk := range objDe.Blocks {
		objBlkMap[blk.Index] = blk
	}

	var compBlks []stgmod.ObjectBlock
	var compBlkStgs []stgmod.StorageDetail
	var targetStg stgmod.StorageDetail
	for i, idx := range indexes {
		blk, ok := objBlkMap[idx]
		if !ok {
			return cdssdk.Object{}, fmt.Errorf("block %d not found in object %v", idx, objectID)
		}

		stg := svc.StorageMeta.Get(blk.StorageID)
		if stg == nil {
			return cdssdk.Object{}, fmt.Errorf("storage %d not found", blk.StorageID)
		}

		compBlks = append(compBlks, blk)
		compBlkStgs = append(compBlkStgs, *stg)
		if i == 0 {
			targetStg = *stg
		}
	}

	bld := exec.NewPlanBuilder()
	err = plans.CompleteMultipart(compBlks, compBlkStgs, targetStg, "shard", bld)
	if err != nil {
		return cdssdk.Object{}, err
	}

	exeCtx := exec.NewExecContext()
	ret, err := bld.Execute(exeCtx).Wait(context.Background())
	if err != nil {
		return cdssdk.Object{}, err
	}

	shardInfo := ret["shard"].(*ops2.ShardInfoValue)
	_, err = coorCli.UpdateObjectRedundancy(coormq.ReqUpdateObjectRedundancy([]coormq.UpdatingObjectRedundancy{
		{
			ObjectID:   objectID,
			FileHash:   shardInfo.Hash,
			Size:       shardInfo.Size,
			Redundancy: cdssdk.NewNoneRedundancy(),
			Blocks: []stgmod.ObjectBlock{{
				ObjectID:  objectID,
				Index:     0,
				StorageID: targetStg.Storage.StorageID,
				FileHash:  shardInfo.Hash,
				Size:      shardInfo.Size,
			}},
		},
	}))

	if err != nil {
		return cdssdk.Object{}, err
	}

	getObj, err := coorCli.GetObjects(coormq.ReqGetObjects(userID, []cdssdk.ObjectID{objectID}))
	if err != nil {
		return cdssdk.Object{}, err
	}

	if getObj.Objects[0] == nil {
		return cdssdk.Object{}, fmt.Errorf("object %v not found", objectID)
	}

	return *getObj.Objects[0], nil
}
