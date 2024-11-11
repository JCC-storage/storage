package types

import (
	"fmt"
	"io"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
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
	StorageComponent
	// 准备写入一个新文件，写入后获得FileHash
	New() ShardWriter
	// 使用F函数创建Option对象
	Open(opt OpenOption) (io.ReadCloser, error)
	// 获得指定文件信息
	Info(fileHash cdssdk.FileHash) (FileInfo, error)
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

type ShardWriter interface {
	io.Writer
	// 取消写入。要求允许在调用了Finish之后再调用此函数，且此时不应该有任何影响，方便使用defer语句
	Abort() error
	// 结束写入，获得文件哈希值
	Finish() (FileInfo, error)
}

type OpenOption struct {
	FileHash cdssdk.FileHash
	Offset   int64
	Length   int64
}

func NewOpen(fileHash cdssdk.FileHash) OpenOption {
	return OpenOption{
		FileHash: fileHash,
		Offset:   0,
		Length:   -1,
	}
}

func (o *OpenOption) WithLength(len int64) OpenOption {
	o.Length = len
	return *o
}

// [start, end]，即包含end
func (o *OpenOption) WithRange(start int64, end int64) OpenOption {
	o.Offset = start
	o.Length = end - start + 1
	return *o
}

func (o *OpenOption) WithNullableLength(offset int64, length *int64) {
	o.Offset = offset
	if length != nil {
		o.Length = *length
	}
}

func (o *OpenOption) String() string {
	rangeStart := ""
	if o.Offset > 0 {
		rangeStart = fmt.Sprintf("%d", o.Offset)
	}

	rangeEnd := ""
	if o.Length >= 0 {
		rangeEnd = fmt.Sprintf("%d", o.Offset+o.Length-1)
	}

	if rangeStart == "" && rangeEnd == "" {
		return string(o.FileHash)
	}

	return fmt.Sprintf("%s[%s:%s]", string(o.FileHash), rangeStart, rangeEnd)
}
