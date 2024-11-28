package factory

import (
	"reflect"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

// 创建一个在MasterHub上长期运行的存储服务
type StorageServiceBuilder func(detail stgmod.StorageDetail) (types.StorageService, error)

// 根据存储服务信息创建一个指定类型的组件
type StorageComponentBuilder func(detail stgmod.StorageDetail, typ reflect.Type) (any, error)

type StorageBuilder struct {
	CreateService   StorageServiceBuilder
	CreateComponent StorageComponentBuilder
}

var storageBuilders = make(map[reflect.Type]StorageBuilder)

// 注册针对指定存储服务类型的Builder
func RegisterBuilder[T cdssdk.StorageType](createSvc StorageServiceBuilder, createComp StorageComponentBuilder) {
	storageBuilders[reflect2.TypeOf[T]()] = StorageBuilder{
		CreateService:   createSvc,
		CreateComponent: createComp,
	}
}

func CreateService(detail stgmod.StorageDetail) (types.StorageService, error) {
	typ := reflect.TypeOf(detail.Storage.Type)
	bld, ok := storageBuilders[typ]
	if !ok {
		return nil, types.ErrUnsupportedStorageType
	}

	return bld.CreateService(detail)
}

func CreateComponent[T any](detail stgmod.StorageDetail) (T, error) {
	typ := reflect.TypeOf(detail.Storage.Type)
	bld, ok := storageBuilders[typ]
	if !ok {
		var def T
		return def, types.ErrUnsupportedStorageType
	}

	comp, err := bld.CreateComponent(detail, reflect2.TypeOf[T]())
	if err != nil {
		var def T
		return def, err
	}

	return comp.(T), nil
}
