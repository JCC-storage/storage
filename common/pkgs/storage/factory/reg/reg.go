package reg

import (
	"reflect"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type BuilderCtor func(detail stgmod.StorageDetail) types.StorageBuilder

var StorageBuilders = make(map[reflect.Type]BuilderCtor)

// 注册针对指定存储服务类型的Builder
func RegisterBuilder[T cdssdk.StorageType](ctor BuilderCtor) {
	StorageBuilders[reflect2.TypeOf[T]()] = ctor
}

// 注：此函数只给storage包内部使用，外部包请使用外层的factory.GetBuilder
// 此函数永远不会返回nil。如果找不到对应的Builder，则会返回EmptyBuilder，
// 此Builder的所有函数都会返回否定值或者封装后的ErrUnsupported错误（需要使用errors.Is检查）
func GetBuilderInternal(detail stgmod.StorageDetail) types.StorageBuilder {
	typ := reflect.TypeOf(detail.Storage.Type)

	ctor, ok := StorageBuilders[typ]
	if !ok {
		return &types.EmptyBuilder{}
	}

	return ctor(detail)
}
