package types

import (
	"context"
	"io"
)

type Multiparter interface {
	MaxPartSize() int64
	MinPartSize() int64
	// 启动一个分片上传
	Initiate(ctx context.Context) (MultipartTask, error)
	// 上传一个分片
	UploadPart(ctx context.Context, init MultipartInitState, partSize int64, partNumber int, stream io.Reader) (UploadedPartInfo, error)
}

type MultipartTask interface {
	InitState() MultipartInitState
	// 所有分片上传完成后，合并分片
	JoinParts(ctx context.Context, parts []UploadedPartInfo) (BypassUploadedFile, error)
	// 合成之后的文件已被使用
	Complete()
	// 取消上传。如果在调用Complete之前调用，则应该删除合并后的文件。如果已经调用Complete，则应该不做任何事情。
	Abort()
}

// TODO 可以考虑重构成一个接口，支持不同的类型的分片有不同内容的实现
type MultipartInitState struct {
	UploadID string
	Bucket   string
	Key      string
}

type UploadedPartInfo struct {
	ETag       string
	PartNumber int
	PartHash   []byte
}
