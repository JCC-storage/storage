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

// 在MasterHub上运行，代理一个存储服务。
//
// 存放Storage的运行时数据。如果一个组件需要与Agent交互（比如实际是ShardStore功能的一部分），或者是需要长期运行，
// 那么就将该组件的Get函数放到StorageAgent接口中。可以同时在StorageBuilder中同时提供HasXXX函数，
// 用于判断该Storage是否支持某个功能，用于生成ioswitch计划时判断是否能利用此功能。
type StorageAgent interface {
	Start(ch *StorageEventChan)
	Stop()

	Info() stgmod.StorageDetail
	// 获取分片存储服务
	GetShardStore() (ShardStore, error)
	// 获取共享存储服务
	GetSharedStore() (SharedStore, error)
}

// 创建存储服务的指定组件。
//
// 如果指定组件比较独立，不需要依赖运行时数据，或者不需要与Agent交互，那么就可以将Create函数放到这个接口中。
// 增加Has函数用于判断该Storage是否有某个组件。
// 如果Create函数仅仅只是创建一个结构体，没有其他副作用，那么也可以用Create函数来判断是否支持某个功能。
type StorageBuilder interface {
	// 创建一个在MasterHub上长期运行的存储服务
	CreateAgent() (StorageAgent, error)
	// 是否支持分片存储服务
	HasShardStore() bool
	// 是否支持共享存储服务
	HasSharedStore() bool
	// 创建一个分片上传组件
	CreateMultiparter() (Multiparter, error)
}
