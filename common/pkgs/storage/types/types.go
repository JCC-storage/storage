package types

import (
	"errors"
	"reflect"

	"gitlink.org.cn/cloudream/common/pkgs/async"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

var ErrStorageNotFound = errors.New("storage not found")

var ErrComponentNotFound = errors.New("component not found")

var ErrStorageExists = errors.New("storage already exists")

type StorageEvent interface{}

type StorageEventChan = async.UnboundChannel[StorageEvent]

// 代表一个长期运行在MasterHub上的存储服务
type StorageService interface {
	Info() stgmod.StorageDetail
	GetComponent(typ reflect.Type) (any, error)
	Start(ch *StorageEventChan)
	Stop()
}

// 创建一个在MasterHub上长期运行的存储服务
type StorageServiceBuilder func(detail stgmod.StorageDetail) (StorageService, error)

// 根据存储服务信息创建一个指定类型的组件
type StorageComponentBuilder func(detail stgmod.StorageDetail, typ reflect.Type) (any, error)

type StorageBuilder struct {
	CreateService   StorageServiceBuilder
	CreateComponent StorageComponentBuilder
}
