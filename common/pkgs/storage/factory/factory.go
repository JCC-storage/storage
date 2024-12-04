package factory

import (
	"fmt"
	"reflect"

	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"

	// 需要导入所有存储服务的包
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/storage/local"
)

func CreateService(detail stgmod.StorageDetail) (types.StorageService, error) {
	typ := reflect.TypeOf(detail.Storage.Type)
	bld, ok := reg.StorageBuilders[typ]
	if !ok {
		return nil, fmt.Errorf("unsupported storage type: %T", detail.Storage.Type)
	}

	return bld.CreateService(detail)
}

func CreateComponent[T any](detail stgmod.StorageDetail) (T, error) {
	typ := reflect.TypeOf(detail.Storage.Type)
	bld, ok := reg.StorageBuilders[typ]
	if !ok {
		var def T
		return def, fmt.Errorf("unsupported storage type: %T", detail.Storage.Type)
	}

	comp, err := bld.CreateComponent(detail, reflect2.TypeOf[T]())
	if err != nil {
		var def T
		return def, err
	}

	c, ok := comp.(T)
	if !ok {
		var def T
		return def, fmt.Errorf("invalid component type: %T", comp)
	}

	return c, nil
}
