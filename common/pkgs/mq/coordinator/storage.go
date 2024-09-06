package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type StorageService interface {
	GetStorage(msg *GetStorage) (*GetStorageResp, *mq.CodeMessage)

	GetStorageByName(msg *GetStorageByName) (*GetStorageByNameResp, *mq.CodeMessage)

	StoragePackageLoaded(msg *StoragePackageLoaded) (*StoragePackageLoadedResp, *mq.CodeMessage)
}

// 获取Storage信息
var _ = Register(Service.GetStorage)

type GetStorage struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	StorageID cdssdk.StorageID `json:"storageID"`
}
type GetStorageResp struct {
	mq.MessageBodyBase
	Storage model.Storage `json:"storage"`
}

func ReqGetStorage(userID cdssdk.UserID, storageID cdssdk.StorageID) *GetStorage {
	return &GetStorage{
		UserID:    userID,
		StorageID: storageID,
	}
}
func RespGetStorage(stg model.Storage) *GetStorageResp {
	return &GetStorageResp{
		Storage: stg,
	}
}
func (client *Client) GetStorage(msg *GetStorage) (*GetStorageResp, error) {
	return mq.Request(Service.GetStorage, client.rabbitCli, msg)
}

var _ = Register(Service.GetStorageByName)

type GetStorageByName struct {
	mq.MessageBodyBase
	UserID cdssdk.UserID `json:"userID"`
	Name   string        `json:"name"`
}
type GetStorageByNameResp struct {
	mq.MessageBodyBase
	Storage model.Storage `json:"storage"`
}

func ReqGetStorageByName(userID cdssdk.UserID, name string) *GetStorageByName {
	return &GetStorageByName{
		UserID: userID,
		Name:   name,
	}
}
func RespGetStorageByNameResp(storage model.Storage) *GetStorageByNameResp {
	return &GetStorageByNameResp{
		Storage: storage,
	}
}
func (client *Client) GetStorageByName(msg *GetStorageByName) (*GetStorageByNameResp, error) {
	return mq.Request(Service.GetStorageByName, client.rabbitCli, msg)
}

// 提交调度记录
var _ = Register(Service.StoragePackageLoaded)

type StoragePackageLoaded struct {
	mq.MessageBodyBase
	UserID       cdssdk.UserID        `json:"userID"`
	StorageID    cdssdk.StorageID     `json:"storageID"`
	PackageID    cdssdk.PackageID     `json:"packageID"`
	PinnedBlocks []stgmod.ObjectBlock `json:"pinnedBlocks"`
}
type StoragePackageLoadedResp struct {
	mq.MessageBodyBase
}

func NewStoragePackageLoaded(userID cdssdk.UserID, stgID cdssdk.StorageID, packageID cdssdk.PackageID, pinnedBlocks []stgmod.ObjectBlock) *StoragePackageLoaded {
	return &StoragePackageLoaded{
		UserID:       userID,
		PackageID:    packageID,
		StorageID:    stgID,
		PinnedBlocks: pinnedBlocks,
	}
}
func NewStoragePackageLoadedResp() *StoragePackageLoadedResp {
	return &StoragePackageLoadedResp{}
}
func (client *Client) StoragePackageLoaded(msg *StoragePackageLoaded) (*StoragePackageLoadedResp, error) {
	return mq.Request(Service.StoragePackageLoaded, client.rabbitCli, msg)
}
