package types

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type BypassFileInfo struct {
	TempFilePath string
	FileHash     cdssdk.FileHash
	Size         int64
}

type BypassNotifier interface {
	BypassUploaded(info BypassFileInfo) error
}
