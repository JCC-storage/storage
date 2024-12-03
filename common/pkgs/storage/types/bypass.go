package types

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// type BypassWriter interface {
// 	Write(stream io.Reader) (string, error)
// }

type BypassFileInfo struct {
	TempFilePath string
	FileHash     cdssdk.FileHash
}

type BypassNotifier interface {
	BypassUploaded(info BypassFileInfo) error
}
