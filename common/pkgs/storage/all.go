package storage

import (
	// !!! 需要导入所有存储服务的包 !!!
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/storage/efile"
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/storage/local"
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/storage/mashup"
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/storage/obs"
)
