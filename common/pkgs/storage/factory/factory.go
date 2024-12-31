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

func GetBuilder(detail stgmod.StorageDetail) types.StorageBuilder {
	typ := reflect.TypeOf(detail.Storage.Type)
	return reg.StorageBuilders[typ]
}
