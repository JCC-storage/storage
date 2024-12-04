package types

import (
	"context"
	"io"
)

type MultipartInitiator interface {
	// 启动一个分片上传
	Initiate(ctx context.Context) (MultipartInitState, error)
	// 所有分片上传完成后，合并分片
	JoinParts(ctx context.Context, parts []UploadedPartInfo) (BypassFileInfo, error)
	// 合成之后的文件已被使用
	Complete()
	// 取消上传。如果在调用Complete之前调用，则应该删除合并后的文件。如果已经调用Complete，则应该不做任何事情。
	Abort()
}

type MultipartUploader interface {
	UploadPart(ctx context.Context, init MultipartInitState, partSize int64, partNumber int, stream io.Reader) (UploadedPartInfo, error)
	Close()
}

type MultipartInitState struct {
	UploadID string
}

type UploadedPartInfo struct {
	PartNumber int
	ETag       string
}
