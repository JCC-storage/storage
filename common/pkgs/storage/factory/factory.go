package factory

import (
	"reflect"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"

	// !!! 需要导入所有存储服务的包 !!!
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/storage/local"
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/storage/s3"
)

// 此函数永远不会返回nil。如果找不到对应的Builder，则会返回EmptyBuilder，
// 此Builder的所有函数都会返回否定值或者封装后的ErrUnsupported错误（需要使用errors.Is检查）
func GetBuilder(detail stgmod.StorageDetail) types.StorageBuilder {
	typ := reflect.TypeOf(detail.Storage.Type)

	ctor, ok := reg.StorageBuilders[typ]
	if !ok {
		return &types.EmptyBuilder{}
	}

	return ctor(detail)
}
