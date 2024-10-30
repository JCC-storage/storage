package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type StorageService interface {
	GetStorage(msg *GetStorage) (*GetStorageResp, *mq.CodeMessage)

	GetStorageDetails(msg *GetStorageDetails) (*GetStorageDetailsResp, *mq.CodeMessage)

	GetUserStorageDetails(msg *GetUserStorageDetails) (*GetUserStorageDetailsResp, *mq.CodeMessage)

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

// 获取Storage的详细信息
var _ = Register(Service.GetStorageDetails)

type GetStorageDetails struct {
	mq.MessageBodyBase
	StorageIDs []cdssdk.StorageID `json:"storageIDs"`
}
type GetStorageDetailsResp struct {
	mq.MessageBodyBase
	Storages []*stgmod.StorageDetail `json:"storages"`
}

func ReqGetStorageDetails(storageIDs []cdssdk.StorageID) *GetStorageDetails {
	return &GetStorageDetails{
		StorageIDs: storageIDs,
	}
}
func RespGetStorageDetails(stgs []*stgmod.StorageDetail) *GetStorageDetailsResp {
	return &GetStorageDetailsResp{
		Storages: stgs,
	}
}
func (client *Client) GetStorageDetails(msg *GetStorageDetails) (*GetStorageDetailsResp, error) {
	return mq.Request(Service.GetStorageDetails, client.rabbitCli, msg)
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

// 获取用户的Storage信息
var _ = Register(Service.GetUserStorageDetails)

type GetUserStorageDetails struct {
	mq.MessageBodyBase
	UserID cdssdk.UserID `json:"userID"`
}
type GetUserStorageDetailsResp struct {
	mq.MessageBodyBase
	Storages []stgmod.StorageDetail `json:"storages"`
}

func ReqGetUserStorageDetails(userID cdssdk.UserID) *GetUserStorageDetails {
	return &GetUserStorageDetails{
		UserID: userID,
	}
}
func RespGetUserStorageDetails(stgs []stgmod.StorageDetail) *GetUserStorageDetailsResp {
	return &GetUserStorageDetailsResp{
		Storages: stgs,
	}
}
func (client *Client) GetUserStorageDetails(msg *GetUserStorageDetails) (*GetUserStorageDetailsResp, error) {
	return mq.Request(Service.GetUserStorageDetails, client.rabbitCli, msg)
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
