package types

import (
	"context"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type S2STransfer interface {
	// 判断是否能从指定的源存储中直传到当前存储的目的路径
	CanTransfer(src stgmod.StorageDetail) bool
	// 执行数据直传。返回传输后的文件路径
	Transfer(ctx context.Context, src stgmod.StorageDetail, srcPath string) (string, error)
	// 完成传输
	Complete()
	// 取消传输。如果已经调用了Complete，则这个方法应该无效果
	Abort()
}
