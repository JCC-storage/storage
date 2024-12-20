package coordinator

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type PackageService interface {
	GetPackage(msg *GetPackage) (*GetPackageResp, *mq.CodeMessage)

	GetPackageByName(msg *GetPackageByName) (*GetPackageByNameResp, *mq.CodeMessage)

	CreatePackage(msg *CreatePackage) (*CreatePackageResp, *mq.CodeMessage)

	UpdatePackage(msg *UpdatePackage) (*UpdatePackageResp, *mq.CodeMessage)

	DeletePackage(msg *DeletePackage) (*DeletePackageResp, *mq.CodeMessage)

	ClonePackage(msg *ClonePackage) (*ClonePackageResp, *mq.CodeMessage)

	GetPackageCachedStorages(msg *GetPackageCachedStorages) (*GetPackageCachedStoragesResp, *mq.CodeMessage)

	GetPackageLoadedStorages(msg *GetPackageLoadedStorages) (*GetPackageLoadedStoragesResp, *mq.CodeMessage)
}

// 获取Package基本信息
var _ = Register(Service.GetPackage)

type GetPackage struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageResp struct {
	mq.MessageBodyBase
	model.Package
}

func NewGetPackage(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackage {
	return &GetPackage{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewGetPackageResp(pkg model.Package) *GetPackageResp {
	return &GetPackageResp{
		Package: pkg,
	}
}

func (client *Client) GetPackage(msg *GetPackage) (*GetPackageResp, error) {
	return mq.Request(Service.GetPackage, client.rabbitCli, msg)
}

// 根据名称获取Package
var _ = Register(Service.GetPackageByName)

type GetPackageByName struct {
	mq.MessageBodyBase
	UserID      cdssdk.UserID `json:"userID"`
	BucketName  string        `json:"bucketName"`
	PackageName string        `json:"packageName"`
}
type GetPackageByNameResp struct {
	mq.MessageBodyBase
	Package cdssdk.Package `json:"package"`
}

func ReqGetPackageByName(userID cdssdk.UserID, bucketName string, packageName string) *GetPackageByName {
	return &GetPackageByName{
		UserID:      userID,
		BucketName:  bucketName,
		PackageName: packageName,
	}
}
func NewGetPackageByNameResp(pkg cdssdk.Package) *GetPackageByNameResp {
	return &GetPackageByNameResp{
		Package: pkg,
	}
}
func (client *Client) GetPackageByName(msg *GetPackageByName) (*GetPackageByNameResp, error) {
	return mq.Request(Service.GetPackageByName, client.rabbitCli, msg)
}

// 创建一个Package
var _ = Register(Service.CreatePackage)

type CreatePackage struct {
	mq.MessageBodyBase
	UserID   cdssdk.UserID   `json:"userID"`
	BucketID cdssdk.BucketID `json:"bucketID"`
	Name     string          `json:"name"`
}
type CreatePackageResp struct {
	mq.MessageBodyBase
	Package cdssdk.Package `json:"package"`
}

func NewCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string) *CreatePackage {
	return &CreatePackage{
		UserID:   userID,
		BucketID: bucketID,
		Name:     name,
	}
}
func NewCreatePackageResp(pkg cdssdk.Package) *CreatePackageResp {
	return &CreatePackageResp{
		Package: pkg,
	}
}

func (client *Client) CreatePackage(msg *CreatePackage) (*CreatePackageResp, error) {
	return mq.Request(Service.CreatePackage, client.rabbitCli, msg)
}

// 更新Package
var _ = Register(Service.UpdatePackage)

type UpdatePackage struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID  `json:"packageID"`
	Adds      []AddObjectEntry  `json:"adds"`
	Deletes   []cdssdk.ObjectID `json:"deletes"`
}
type UpdatePackageResp struct {
	mq.MessageBodyBase
	Added []cdssdk.Object `json:"added"`
}
type AddObjectEntry struct {
	Path       string             `json:"path"`
	Size       int64              `json:"size,string"`
	FileHash   cdssdk.FileHash    `json:"fileHash"`
	UploadTime time.Time          `json:"uploadTime"` // 开始上传文件的时间
	StorageIDs []cdssdk.StorageID `json:"storageIDs"`
}

func NewUpdatePackage(packageID cdssdk.PackageID, adds []AddObjectEntry, deletes []cdssdk.ObjectID) *UpdatePackage {
	return &UpdatePackage{
		PackageID: packageID,
		Adds:      adds,
		Deletes:   deletes,
	}
}
func NewUpdatePackageResp(added []cdssdk.Object) *UpdatePackageResp {
	return &UpdatePackageResp{
		Added: added,
	}
}
func NewAddObjectEntry(path string, size int64, fileHash cdssdk.FileHash, uploadTime time.Time, stgIDs []cdssdk.StorageID) AddObjectEntry {
	return AddObjectEntry{
		Path:       path,
		Size:       size,
		FileHash:   fileHash,
		UploadTime: uploadTime,
		StorageIDs: stgIDs,
	}
}
func (client *Client) UpdatePackage(msg *UpdatePackage) (*UpdatePackageResp, error) {
	return mq.Request(Service.UpdatePackage, client.rabbitCli, msg)
}

// 删除对象
var _ = Register(Service.DeletePackage)

type DeletePackage struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}
type DeletePackageResp struct {
	mq.MessageBodyBase
}

func NewDeletePackage(userID cdssdk.UserID, packageID cdssdk.PackageID) *DeletePackage {
	return &DeletePackage{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewDeletePackageResp() *DeletePackageResp {
	return &DeletePackageResp{}
}
func (client *Client) DeletePackage(msg *DeletePackage) (*DeletePackageResp, error) {
	return mq.Request(Service.DeletePackage, client.rabbitCli, msg)
}

// 克隆Package
var _ = Register(Service.ClonePackage)

type ClonePackage struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
	BucketID  cdssdk.BucketID  `json:"bucketID"`
	Name      string           `json:"name"`
}
type ClonePackageResp struct {
	mq.MessageBodyBase
	Package cdssdk.Package `json:"package"`
}

func ReqClonePackage(userID cdssdk.UserID, packageID cdssdk.PackageID, bucketID cdssdk.BucketID, name string) *ClonePackage {
	return &ClonePackage{
		UserID:    userID,
		PackageID: packageID,
		BucketID:  bucketID,
		Name:      name,
	}
}
func RespClonePackage(pkg cdssdk.Package) *ClonePackageResp {
	return &ClonePackageResp{
		Package: pkg,
	}
}

func (client *Client) ClonePackage(msg *ClonePackage) (*ClonePackageResp, error) {
	return mq.Request(Service.ClonePackage, client.rabbitCli, msg)
}

// 根据PackageID获取object分布情况
var _ = Register(Service.GetPackageCachedStorages)

type GetPackageCachedStorages struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}

type PackageCachedStorageInfo struct {
	StorageID   int64 `json:"storageID"`
	FileSize    int64 `json:"fileSize"`
	ObjectCount int64 `json:"objectCount"`
}

type GetPackageCachedStoragesResp struct {
	mq.MessageBodyBase
	cdssdk.PackageCachingInfo
}

func ReqGetPackageCachedStorages(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackageCachedStorages {
	return &GetPackageCachedStorages{
		UserID:    userID,
		PackageID: packageID,
	}
}

func ReqGetPackageCachedStoragesResp(stgInfos []cdssdk.StoragePackageCachingInfo, packageSize int64) *GetPackageCachedStoragesResp {
	return &GetPackageCachedStoragesResp{
		PackageCachingInfo: cdssdk.PackageCachingInfo{
			StorageInfos: stgInfos,
			PackageSize:  packageSize,
		},
	}
}

func (client *Client) GetPackageCachedStorages(msg *GetPackageCachedStorages) (*GetPackageCachedStoragesResp, error) {
	return mq.Request(Service.GetPackageCachedStorages, client.rabbitCli, msg)
}

// 根据PackageID获取storage分布情况
var _ = Register(Service.GetPackageLoadedStorages)

type GetPackageLoadedStorages struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}

type GetPackageLoadedStoragesResp struct {
	mq.MessageBodyBase
	StorageIDs []cdssdk.StorageID `json:"storageIDs"`
}

func ReqGetPackageLoadedStorages(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackageLoadedStorages {
	return &GetPackageLoadedStorages{
		UserID:    userID,
		PackageID: packageID,
	}
}

func NewGetPackageLoadedStoragesResp(stgIDs []cdssdk.StorageID) *GetPackageLoadedStoragesResp {
	return &GetPackageLoadedStoragesResp{
		StorageIDs: stgIDs,
	}
}

func (client *Client) GetPackageLoadedStorages(msg *GetPackageLoadedStorages) (*GetPackageLoadedStoragesResp, error) {
	return mq.Request(Service.GetPackageLoadedStorages, client.rabbitCli, msg)
}
