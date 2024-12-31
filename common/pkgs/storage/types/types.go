package types

import (
	"errors"

	"gitlink.org.cn/cloudream/common/pkgs/async"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

var ErrStorageNotFound = errors.New("storage not found")

// 不支持的操作。可以作为StorageBuilder中任意函数的错误返回值，代表该操作不被支持。
var ErrUnsupported = errors.New("unsupported operation")

var ErrStorageExists = errors.New("storage already exists")

type StorageEvent interface{}

type StorageEventChan = async.UnboundChannel[StorageEvent]

/*
如果一个组件需要与Agent交互（比如实际是ShardStore功能的一部分），那么就将Create函数放到StorageAgent接口中。
如果组件十分独立，仅需要存储服务的配置信息就行，那么就把Create函数放到StorageBuilder中去。
*/

// 在MasterHub上运行，代理一个存储服务。
type StorageAgent interface {
	Start(ch *StorageEventChan)
	Stop()

	Info() stgmod.StorageDetail
	// 获取分片存储服务
	GetShardStore() (ShardStore, error)
	// 获取共享存储服务
	GetSharedStore() (SharedStore, error)
}

// 创建存储服务的指定组件
type StorageBuilder interface {
	// 创建一个在MasterHub上长期运行的存储服务
	CreateAgent(detail stgmod.StorageDetail) (StorageAgent, error)
	// 创建一个分片上传功能的初始化器
	CreateMultipartInitiator(detail stgmod.StorageDetail) (MultipartInitiator, error)
	// 创建一个分片上传功能的上传器
	CreateMultipartUploader(detail stgmod.StorageDetail) (MultipartUploader, error)
}
