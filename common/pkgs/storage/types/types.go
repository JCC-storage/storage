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

var ErrUnsupportedStorageType = errors.New("unsupported storage type")

var ErrUnsupportedComponent = errors.New("unsupported component type")

type StorageEvent interface{}

type StorageEventChan = async.UnboundChannel[StorageEvent]

// 代表一个长期运行在MasterHub上的存储服务
type StorageService interface {
	Info() stgmod.StorageDetail
	GetComponent(typ reflect.Type) (any, error)
	Start(ch *StorageEventChan)
	Stop()
}
