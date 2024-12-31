package types

import stgmod "gitlink.org.cn/cloudream/storage/common/models"

type S2STransfer interface {
	Transfer(src stgmod.StorageDetail, srcPath string, dstPath string) error
}
