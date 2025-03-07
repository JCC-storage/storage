package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
)

type ObjectService interface {
	GetObjects(msg *GetObjects) (*GetObjectsResp, *mq.CodeMessage)

	GetObjectsByPath(msg *GetObjectsByPath) (*GetObjectsByPathResp, *mq.CodeMessage)

	GetPackageObjects(msg *GetPackageObjects) (*GetPackageObjectsResp, *mq.CodeMessage)

	GetPackageObjectDetails(msg *GetPackageObjectDetails) (*GetPackageObjectDetailsResp, *mq.CodeMessage)

	GetObjectDetails(msg *GetObjectDetails) (*GetObjectDetailsResp, *mq.CodeMessage)

	UpdateObjectRedundancy(msg *UpdateObjectRedundancy) (*UpdateObjectRedundancyResp, *mq.CodeMessage)

	UpdateObjectInfos(msg *UpdateObjectInfos) (*UpdateObjectInfosResp, *mq.CodeMessage)

	MoveObjects(msg *MoveObjects) (*MoveObjectsResp, *mq.CodeMessage)

	DeleteObjects(msg *DeleteObjects) (*DeleteObjectsResp, *mq.CodeMessage)

	CloneObjects(msg *CloneObjects) (*CloneObjectsResp, *mq.CodeMessage)

	GetDatabaseAll(msg *GetDatabaseAll) (*GetDatabaseAllResp, *mq.CodeMessage)

	AddAccessStat(msg *AddAccessStat)

	NewMultipartUploadObject(msg *NewMultipartUploadObject) (*NewMultipartUploadObjectResp, *mq.CodeMessage)

	AddMultipartUploadPart(msg *AddMultipartUploadPart) (*AddMultipartUploadPartResp, *mq.CodeMessage)
}

var _ = Register(Service.GetObjects)

type GetObjects struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID     `json:"userID"`
	ObjectIDs []cdssdk.ObjectID `json:"objectIDs"`
}
type GetObjectsResp struct {
	mq.MessageBodyBase
	Objects []*cdssdk.Object `json:"objects"`
}

func ReqGetObjects(userID cdssdk.UserID, objectIDs []cdssdk.ObjectID) *GetObjects {
	return &GetObjects{
		UserID:    userID,
		ObjectIDs: objectIDs,
	}
}
func RespGetObjects(objects []*cdssdk.Object) *GetObjectsResp {
	return &GetObjectsResp{
		Objects: objects,
	}
}
func (client *Client) GetObjects(msg *GetObjects) (*GetObjectsResp, error) {
	return mq.Request(Service.GetObjects, client.rabbitCli, msg)
}

// 查询指定前缀的Object，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetObjectsByPath)

type GetObjectsByPath struct {
	mq.MessageBodyBase
	UserID      cdssdk.UserID    `json:"userID"`
	PackageID   cdssdk.PackageID `json:"packageID"`
	Path        string           `json:"path"`
	IsPrefix    bool             `json:"isPrefix"`
	NoRecursive bool             `json:"noRecursive"`
}
type GetObjectsByPathResp struct {
	mq.MessageBodyBase
	CommonPrefixes []string       `json:"commonPrefixes"`
	Objects        []model.Object `json:"objects"`
}

func ReqGetObjectsByPath(userID cdssdk.UserID, packageID cdssdk.PackageID, path string, isPrefix bool, noRecursive bool) *GetObjectsByPath {
	return &GetObjectsByPath{
		UserID:      userID,
		PackageID:   packageID,
		Path:        path,
		IsPrefix:    isPrefix,
		NoRecursive: noRecursive,
	}
}
func RespGetObjectsByPath(commonPrefixes []string, objects []model.Object) *GetObjectsByPathResp {
	return &GetObjectsByPathResp{
		CommonPrefixes: commonPrefixes,
		Objects:        objects,
	}
}
func (client *Client) GetObjectsByPath(msg *GetObjectsByPath) (*GetObjectsByPathResp, error) {
	return mq.Request(Service.GetObjectsByPath, client.rabbitCli, msg)
}

// 查询Package中的所有Object，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetPackageObjects)

type GetPackageObjects struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageObjectsResp struct {
	mq.MessageBodyBase
	Objects []model.Object `json:"objects"`
}

func ReqGetPackageObjects(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackageObjects {
	return &GetPackageObjects{
		UserID:    userID,
		PackageID: packageID,
	}
}
func RespGetPackageObjects(objects []model.Object) *GetPackageObjectsResp {
	return &GetPackageObjectsResp{
		Objects: objects,
	}
}
func (client *Client) GetPackageObjects(msg *GetPackageObjects) (*GetPackageObjectsResp, error) {
	return mq.Request(Service.GetPackageObjects, client.rabbitCli, msg)
}

// 获取Package中所有Object以及它们的分块详细信息，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetPackageObjectDetails)

type GetPackageObjectDetails struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageObjectDetailsResp struct {
	mq.MessageBodyBase
	Objects []stgmod.ObjectDetail `json:"objects"`
}

func ReqGetPackageObjectDetails(packageID cdssdk.PackageID) *GetPackageObjectDetails {
	return &GetPackageObjectDetails{
		PackageID: packageID,
	}
}
func RespPackageObjectDetails(objects []stgmod.ObjectDetail) *GetPackageObjectDetailsResp {
	return &GetPackageObjectDetailsResp{
		Objects: objects,
	}
}
func (client *Client) GetPackageObjectDetails(msg *GetPackageObjectDetails) (*GetPackageObjectDetailsResp, error) {
	return mq.Request(Service.GetPackageObjectDetails, client.rabbitCli, msg)
}

// 获取多个Object以及它们的分块详细信息，返回的Objects会按照ObjectID升序。
var _ = Register(Service.GetObjectDetails)

type GetObjectDetails struct {
	mq.MessageBodyBase
	ObjectIDs []cdssdk.ObjectID `json:"objectIDs"`
}
type GetObjectDetailsResp struct {
	mq.MessageBodyBase
	Objects []*stgmod.ObjectDetail `json:"objects"` // 如果没有查询到某个ID对应的信息，则此数组对应位置为nil
}

func ReqGetObjectDetails(objectIDs []cdssdk.ObjectID) *GetObjectDetails {
	return &GetObjectDetails{
		ObjectIDs: objectIDs,
	}
}
func RespGetObjectDetails(objects []*stgmod.ObjectDetail) *GetObjectDetailsResp {
	return &GetObjectDetailsResp{
		Objects: objects,
	}
}
func (client *Client) GetObjectDetails(msg *GetObjectDetails) (*GetObjectDetailsResp, error) {
	return mq.Request(Service.GetObjectDetails, client.rabbitCli, msg)
}

// 更新Object的冗余方式
var _ = Register(Service.UpdateObjectRedundancy)

type UpdateObjectRedundancy struct {
	mq.MessageBodyBase
	Updatings []UpdatingObjectRedundancy `json:"updatings"`
}
type UpdateObjectRedundancyResp struct {
	mq.MessageBodyBase
}
type UpdatingObjectRedundancy struct {
	ObjectID   cdssdk.ObjectID      `json:"objectID"`
	FileHash   cdssdk.FileHash      `json:"fileHash"`
	Size       int64                `json:"size"`
	Redundancy cdssdk.Redundancy    `json:"redundancy"`
	PinnedAt   []cdssdk.StorageID   `json:"pinnedAt"`
	Blocks     []stgmod.ObjectBlock `json:"blocks"`
}

func ReqUpdateObjectRedundancy(updatings []UpdatingObjectRedundancy) *UpdateObjectRedundancy {
	return &UpdateObjectRedundancy{
		Updatings: updatings,
	}
}
func RespUpdateObjectRedundancy() *UpdateObjectRedundancyResp {
	return &UpdateObjectRedundancyResp{}
}
func (client *Client) UpdateObjectRedundancy(msg *UpdateObjectRedundancy) (*UpdateObjectRedundancyResp, error) {
	return mq.Request(Service.UpdateObjectRedundancy, client.rabbitCli, msg)
}

// 更新Object元数据
var _ = Register(Service.UpdateObjectInfos)

type UpdateObjectInfos struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID           `json:"userID"`
	Updatings []cdsapi.UpdatingObject `json:"updatings"`
}

type UpdateObjectInfosResp struct {
	mq.MessageBodyBase
	Successes []cdssdk.ObjectID `json:"successes"`
}

func ReqUpdateObjectInfos(userID cdssdk.UserID, updatings []cdsapi.UpdatingObject) *UpdateObjectInfos {
	return &UpdateObjectInfos{
		UserID:    userID,
		Updatings: updatings,
	}
}
func RespUpdateObjectInfos(successes []cdssdk.ObjectID) *UpdateObjectInfosResp {
	return &UpdateObjectInfosResp{
		Successes: successes,
	}
}
func (client *Client) UpdateObjectInfos(msg *UpdateObjectInfos) (*UpdateObjectInfosResp, error) {
	return mq.Request(Service.UpdateObjectInfos, client.rabbitCli, msg)
}

// 移动Object
var _ = Register(Service.MoveObjects)

type MoveObjects struct {
	mq.MessageBodyBase
	UserID  cdssdk.UserID         `json:"userID"`
	Movings []cdsapi.MovingObject `json:"movings"`
}

type MoveObjectsResp struct {
	mq.MessageBodyBase
	Successes []cdssdk.ObjectID `json:"successes"`
}

func ReqMoveObjects(userID cdssdk.UserID, movings []cdsapi.MovingObject) *MoveObjects {
	return &MoveObjects{
		UserID:  userID,
		Movings: movings,
	}
}
func RespMoveObjects(successes []cdssdk.ObjectID) *MoveObjectsResp {
	return &MoveObjectsResp{
		Successes: successes,
	}
}
func (client *Client) MoveObjects(msg *MoveObjects) (*MoveObjectsResp, error) {
	return mq.Request(Service.MoveObjects, client.rabbitCli, msg)
}

// 删除Object
var _ = Register(Service.DeleteObjects)

type DeleteObjects struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID     `json:"userID"`
	ObjectIDs []cdssdk.ObjectID `json:"objectIDs"`
}

type DeleteObjectsResp struct {
	mq.MessageBodyBase
	Successes []cdssdk.ObjectID `json:"successes"`
}

func ReqDeleteObjects(userID cdssdk.UserID, objectIDs []cdssdk.ObjectID) *DeleteObjects {
	return &DeleteObjects{
		UserID:    userID,
		ObjectIDs: objectIDs,
	}
}
func RespDeleteObjects(sucs []cdssdk.ObjectID) *DeleteObjectsResp {
	return &DeleteObjectsResp{
		Successes: sucs,
	}
}
func (client *Client) DeleteObjects(msg *DeleteObjects) (*DeleteObjectsResp, error) {
	return mq.Request(Service.DeleteObjects, client.rabbitCli, msg)
}

// 克隆Object
var _ = Register(Service.CloneObjects)

type CloneObjects struct {
	mq.MessageBodyBase
	UserID   cdssdk.UserID          `json:"userID"`
	Clonings []cdsapi.CloningObject `json:"clonings"`
}
type CloneObjectsResp struct {
	mq.MessageBodyBase
	Objects []*cdssdk.Object `json:"objects"`
}

func ReqCloneObjects(userID cdssdk.UserID, clonings []cdsapi.CloningObject) *CloneObjects {
	return &CloneObjects{
		UserID:   userID,
		Clonings: clonings,
	}
}
func RespCloneObjects(objects []*cdssdk.Object) *CloneObjectsResp {
	return &CloneObjectsResp{
		Objects: objects,
	}
}
func (client *Client) CloneObjects(msg *CloneObjects) (*CloneObjectsResp, error) {
	return mq.Request(Service.CloneObjects, client.rabbitCli, msg)
}

// 增加访问计数
var _ = RegisterNoReply(Service.AddAccessStat)

type AddAccessStat struct {
	mq.MessageBodyBase
	Entries []AddAccessStatEntry `json:"entries"`
}

type AddAccessStatEntry struct {
	ObjectID  cdssdk.ObjectID  `json:"objectID"`
	PackageID cdssdk.PackageID `json:"packageID"`
	StorageID cdssdk.StorageID `json:"storageID"`
	Counter   float64          `json:"counter"`
}

func ReqAddAccessStat(entries []AddAccessStatEntry) *AddAccessStat {
	return &AddAccessStat{
		Entries: entries,
	}
}

func (client *Client) AddAccessStat(msg *AddAccessStat) error {
	return mq.Send(Service.AddAccessStat, client.rabbitCli, msg)
}

var _ = Register(Service.NewMultipartUploadObject)

type NewMultipartUploadObject struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
	Path      string           `json:"path"`
}
type NewMultipartUploadObjectResp struct {
	mq.MessageBodyBase
	Object cdssdk.Object `json:"object"`
}

func ReqNewMultipartUploadObject(userID cdssdk.UserID, packageID cdssdk.PackageID, path string) *NewMultipartUploadObject {
	return &NewMultipartUploadObject{
		UserID:    userID,
		PackageID: packageID,
		Path:      path,
	}
}
func RespNewMultipartUploadObject(object cdssdk.Object) *NewMultipartUploadObjectResp {
	return &NewMultipartUploadObjectResp{
		Object: object,
	}
}
func (client *Client) NewMultipartUploadObject(msg *NewMultipartUploadObject) (*NewMultipartUploadObjectResp, error) {
	return mq.Request(Service.NewMultipartUploadObject, client.rabbitCli, msg)
}

var _ = Register(Service.AddMultipartUploadPart)

type AddMultipartUploadPart struct {
	mq.MessageBodyBase
	UserID   cdssdk.UserID      `json:"userID"`
	ObjectID cdssdk.ObjectID    `json:"objectID"`
	Block    stgmod.ObjectBlock `json:"block"`
}

type AddMultipartUploadPartResp struct {
	mq.MessageBodyBase
}

func ReqAddMultipartUploadPart(userID cdssdk.UserID, objectID cdssdk.ObjectID, blk stgmod.ObjectBlock) *AddMultipartUploadPart {
	return &AddMultipartUploadPart{
		UserID:   userID,
		ObjectID: objectID,
		Block:    blk,
	}
}
func RespAddMultipartUploadPart() *AddMultipartUploadPartResp {
	return &AddMultipartUploadPartResp{}
}
func (client *Client) AddMultipartUploadPart(msg *AddMultipartUploadPart) (*AddMultipartUploadPartResp, error) {
	return mq.Request(Service.AddMultipartUploadPart, client.rabbitCli, msg)
}
