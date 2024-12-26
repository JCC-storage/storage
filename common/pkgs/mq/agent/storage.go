package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type StorageService interface {
	StartStorageCreatePackage(msg *StartStorageCreatePackage) (*StartStorageCreatePackageResp, *mq.CodeMessage)

	WaitStorageCreatePackage(msg *WaitStorageCreatePackage) (*WaitStorageCreatePackageResp, *mq.CodeMessage)
}

// 启动从Storage上传Package的任务
var _ = Register(Service.StartStorageCreatePackage)

type StartStorageCreatePackage struct {
	mq.MessageBodyBase
	UserID          cdssdk.UserID    `json:"userID"`
	BucketID        cdssdk.BucketID  `json:"bucketID"`
	Name            string           `json:"name"`
	StorageID       cdssdk.StorageID `json:"storageID"`
	Path            string           `json:"path"`
	StorageAffinity cdssdk.StorageID `json:"storageAffinity"`
}
type StartStorageCreatePackageResp struct {
	mq.MessageBodyBase
	TaskID string `json:"taskID"`
}

func NewStartStorageCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, storageID cdssdk.StorageID, path string, stgAffinity cdssdk.StorageID) *StartStorageCreatePackage {
	return &StartStorageCreatePackage{
		UserID:          userID,
		BucketID:        bucketID,
		Name:            name,
		StorageID:       storageID,
		Path:            path,
		StorageAffinity: stgAffinity,
	}
}
func NewStartStorageCreatePackageResp(taskID string) *StartStorageCreatePackageResp {
	return &StartStorageCreatePackageResp{
		TaskID: taskID,
	}
}
func (client *Client) StartStorageCreatePackage(msg *StartStorageCreatePackage, opts ...mq.RequestOption) (*StartStorageCreatePackageResp, error) {
	return mq.Request(Service.StartStorageCreatePackage, client.rabbitCli, msg, opts...)
}

// 等待从Storage上传Package的任务
var _ = Register(Service.WaitStorageCreatePackage)

type WaitStorageCreatePackage struct {
	mq.MessageBodyBase
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}
type WaitStorageCreatePackageResp struct {
	mq.MessageBodyBase
	IsComplete bool             `json:"isComplete"`
	Error      string           `json:"error"`
	PackageID  cdssdk.PackageID `json:"packageID"`
}

func NewWaitStorageCreatePackage(taskID string, waitTimeoutMs int64) *WaitStorageCreatePackage {
	return &WaitStorageCreatePackage{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}
func NewWaitStorageCreatePackageResp(isComplete bool, err string, packageID cdssdk.PackageID) *WaitStorageCreatePackageResp {
	return &WaitStorageCreatePackageResp{
		IsComplete: isComplete,
		Error:      err,
		PackageID:  packageID,
	}
}
func (client *Client) WaitStorageCreatePackage(msg *WaitStorageCreatePackage, opts ...mq.RequestOption) (*WaitStorageCreatePackageResp, error) {
	return mq.Request(Service.WaitStorageCreatePackage, client.rabbitCli, msg, opts...)
}
