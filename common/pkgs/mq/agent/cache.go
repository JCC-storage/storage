package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type CacheService interface {
	CheckCache(msg *CheckCache) (*CheckCacheResp, *mq.CodeMessage)

	CacheGC(msg *CacheGC) (*CacheGCResp, *mq.CodeMessage)

	StartCacheMovePackage(msg *StartCacheMovePackage) (*StartCacheMovePackageResp, *mq.CodeMessage)
	WaitCacheMovePackage(msg *WaitCacheMovePackage) (*WaitCacheMovePackageResp, *mq.CodeMessage)
}

// 检查节点上的IPFS
var _ = Register(Service.CheckCache)

type CheckCache struct {
	mq.MessageBodyBase
	StorageID cdssdk.StorageID `json:"storageID"`
}
type CheckCacheResp struct {
	mq.MessageBodyBase
	FileHashes []cdssdk.FileHash `json:"fileHashes"`
}

func NewCheckCache(stgID cdssdk.StorageID) *CheckCache {
	return &CheckCache{StorageID: stgID}
}
func NewCheckCacheResp(fileHashes []cdssdk.FileHash) *CheckCacheResp {
	return &CheckCacheResp{
		FileHashes: fileHashes,
	}
}
func (client *Client) CheckCache(msg *CheckCache, opts ...mq.RequestOption) (*CheckCacheResp, error) {
	return mq.Request(Service.CheckCache, client.rabbitCli, msg, opts...)
}

// 清理Cache中不用的文件
var _ = Register(Service.CacheGC)

type CacheGC struct {
	mq.MessageBodyBase
	StorageID cdssdk.StorageID  `json:"storageID"`
	Avaiables []cdssdk.FileHash `json:"avaiables"`
}
type CacheGCResp struct {
	mq.MessageBodyBase
}

func ReqCacheGC(stgID cdssdk.StorageID, avaiables []cdssdk.FileHash) *CacheGC {
	return &CacheGC{
		StorageID: stgID,
		Avaiables: avaiables,
	}
}
func RespCacheGC() *CacheGCResp {
	return &CacheGCResp{}
}
func (client *Client) CacheGC(msg *CacheGC, opts ...mq.RequestOption) (*CacheGCResp, error) {
	return mq.Request(Service.CacheGC, client.rabbitCli, msg, opts...)
}

// 将Package的缓存移动到这个节点
var _ = Register(Service.StartCacheMovePackage)

type StartCacheMovePackage struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
	StorageID cdssdk.StorageID `json:"storageID"`
}
type StartCacheMovePackageResp struct {
	mq.MessageBodyBase
	TaskID string `json:"taskID"`
}

func NewStartCacheMovePackage(userID cdssdk.UserID, packageID cdssdk.PackageID, stgID cdssdk.StorageID) *StartCacheMovePackage {
	return &StartCacheMovePackage{
		UserID:    userID,
		PackageID: packageID,
		StorageID: stgID,
	}
}
func NewStartCacheMovePackageResp(taskID string) *StartCacheMovePackageResp {
	return &StartCacheMovePackageResp{
		TaskID: taskID,
	}
}
func (client *Client) StartCacheMovePackage(msg *StartCacheMovePackage, opts ...mq.RequestOption) (*StartCacheMovePackageResp, error) {
	return mq.Request(Service.StartCacheMovePackage, client.rabbitCli, msg, opts...)
}

// 将Package的缓存移动到这个节点
var _ = Register(Service.WaitCacheMovePackage)

type WaitCacheMovePackage struct {
	mq.MessageBodyBase
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}
type WaitCacheMovePackageResp struct {
	mq.MessageBodyBase
	IsComplete bool   `json:"isComplete"`
	Error      string `json:"error"`
}

func NewWaitCacheMovePackage(taskID string, waitTimeoutMs int64) *WaitCacheMovePackage {
	return &WaitCacheMovePackage{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}
func NewWaitCacheMovePackageResp(isComplete bool, err string) *WaitCacheMovePackageResp {
	return &WaitCacheMovePackageResp{
		IsComplete: isComplete,
		Error:      err,
	}
}
func (client *Client) WaitCacheMovePackage(msg *WaitCacheMovePackage, opts ...mq.RequestOption) (*WaitCacheMovePackageResp, error) {
	return mq.Request(Service.WaitCacheMovePackage, client.rabbitCli, msg, opts...)
}
