package types

import (
	"io"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type Status interface {
	String() string
}

type OKStatus struct{}

func (s *OKStatus) String() string {
	return "OK"
}

var StatusOK = &OKStatus{}

type StoreEvent interface {
}

type ShardStore interface {
	types.StorageComponent
	// 准备写入一个新文件，写入后获得FileHash
	New() Writer
	// 使用F函数创建Option对象
	Open(opt OpenOption) (io.ReadCloser, error)
	// 获取所有文件信息，尽量保证操作是原子的
	ListAll() ([]FileInfo, error)
	// 删除指定的文件
	Purge(removes []cdssdk.FileHash) error
	// 获得存储系统信息
	Stats() Stats
}

type Config interface {
	Build() (ShardStore, error)
}

type FileInfo struct {
	// 文件的SHA256哈希值，全大写的16进制字符串格式
	Hash cdssdk.FileHash
	Size int64
	// 文件描述信息，比如文件名，用于调试
	Description string
}

type Stats struct {
	// 存储服务状态，如果状态正常，此值应该是StatusOK
	Status Status
	// 文件总数
	FileCount int
	// 存储空间总大小
	TotalSize int64
	// 已使用的存储空间大小，可以超过存储空间总大小
	UsedSize int64
	// 描述信息，用于调试
	Description string
}

type Writer interface {
	io.Writer
	// 取消写入。要求允许在调用了Finish之后再调用此函数，且此时不应该有任何影响。
	// 方便defer机制
	Abort() error
	// 结束写入，获得文件哈希值
	Finish() (FileInfo, error)
}
