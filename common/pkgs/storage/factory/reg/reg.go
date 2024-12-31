package reg

import (
	"reflect"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

var StorageBuilders = make(map[reflect.Type]types.StorageBuilder)

// 注册针对指定存储服务类型的Builder
func RegisterBuilder[T cdssdk.StorageType](builder types.StorageBuilder) {
	StorageBuilders[reflect2.TypeOf[T]()] = builder
}
