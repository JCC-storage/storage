package types

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// 通过旁路上传后的文件的信息
type BypassUploadedFile struct {
	Path string
	Hash cdssdk.FileHash
	Size int64
}

// 不通过ShardStore上传文件，但上传完成后需要通知ShardStore。
// 也可以用于共享存储。
type BypassWrite interface {
	BypassUploaded(info BypassUploadedFile) error
}

// 描述指定文件在分片存储中的路径。可以考虑设计成interface。
type BypassFilePath struct {
	Path string
	Info FileInfo
}

// 不通过ShardStore读取文件，但需要它返回文件的路径。
// 仅用于分片存储。
type BypassRead interface {
	BypassRead(fileHash cdssdk.FileHash) (BypassFilePath, error)
}

// 能通过一个Http请求直接访问文件
// 仅用于分片存储。
type HTTPBypassRead interface {
	HTTPBypassRead(fileHash cdssdk.FileHash) (HTTPRequest, error)
}

type HTTPRequest struct {
	URL    string            `json:"url"`
	Method string            `json:"method"`
	Header map[string]string `json:"header"`
	Body   string            `json:"body"`
}
