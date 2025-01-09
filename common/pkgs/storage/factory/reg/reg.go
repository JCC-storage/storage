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
