package services

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	mytask "gitlink.org.cn/cloudream/storage/client/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
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

// StartUploading 开始上传对象。
// userID: 用户ID。
// packageID: 套件ID。
// objIter: 正在上传的对象迭代器。
// nodeAffinity: 节点亲和性，指定对象上传的首选节点。
// 返回值: 任务ID和错误信息。
func (svc *ObjectService) StartUploading(userID cdssdk.UserID, packageID cdssdk.PackageID, objIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) (string, error) {
	tsk := svc.TaskMgr.StartNew(mytask.NewUploadObjects(userID, packageID, objIter, nodeAffinity))
	return tsk.ID(), nil
}

// WaitUploading 等待上传任务完成。
// taskID: 任务ID。
// waitTimeout: 等待超时时间。
// 返回值: 任务是否完成、上传结果和错误信息。
func (svc *ObjectService) WaitUploading(taskID string, waitTimeout time.Duration) (bool, *mytask.UploadObjectsResult, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		updatePkgTask := tsk.Body().(*mytask.UploadObjects)
		return true, updatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *ObjectService) UpdateInfo(userID cdssdk.UserID, updatings []cdssdk.UpdatingObject) ([]cdssdk.ObjectID, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.UpdateObjectInfos(coormq.ReqUpdateObjectInfos(userID, updatings))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return resp.Successes, nil
}

func (svc *ObjectService) Move(userID cdssdk.UserID, movings []cdssdk.MovingObject) ([]cdssdk.ObjectID, error) {
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
	if downloading.Object == nil {
		return nil, fmt.Errorf("object not found")
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

	getResp, err := coorCli.GetPackageObjects(coormq.NewGetPackageObjects(userID, packageID)) // 请求协调器获取套餐对象
	if err != nil {
		return nil, fmt.Errorf("requesting to coordinator: %w", err)
	}

	return getResp.Objects, nil
}
