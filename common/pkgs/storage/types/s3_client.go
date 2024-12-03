package types

import (
	"context"
	"io"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type MultipartInitiator interface {
	MultipartUploader
	Initiate(ctx context.Context, objectName string) (MultipartInitState, error)
	Complete(ctx context.Context, parts []UploadedPartInfo) (CompletedFileInfo, error)
	Abort()
}

type MultipartUploader interface {
	UploadPart(ctx context.Context, init MultipartInitState, objectName string, partSize int64, partNumber int, stream io.Reader) (UploadedPartInfo, error)
	Close()
}

type MultipartInitState struct {
	UploadID string
}

type UploadedPartInfo struct {
	PartNumber int
	ETag       string
}

type CompletedFileInfo struct {
	FileHash cdssdk.FileHash // 可以为空，为空代表获取不到FileHash值
}
