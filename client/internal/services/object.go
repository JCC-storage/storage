package services

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
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

func (svc *ObjectService) GetByPath(userID cdssdk.UserID, pkgID cdssdk.PackageID, path string, isPrefix bool) ([]cdssdk.Object, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	listResp, err := coorCli.GetObjectsByPath(coormq.ReqGetObjectsByPath(userID, pkgID, path, isPrefix))
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
