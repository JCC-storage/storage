package ioswitchlrc

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/math2"
)

type From interface {
	GetDataIndex() int
}

type To interface {
	// To所需要的文件流的范围。具体含义与DataIndex有关系：
	// 如果DataIndex == -1，则表示在整个文件的范围。
	// 如果DataIndex >= 0，则表示在文件的某个分片的范围。
	GetRange() math2.Range
	GetDataIndex() int
}

type FromDriver struct {
	Handle    *exec.DriverWriteStream
	DataIndex int
}

func NewFromDriver(dataIndex int) (*FromDriver, *exec.DriverWriteStream) {
	handle := &exec.DriverWriteStream{
		RangeHint: &math2.Range{},
	}
	return &FromDriver{
		Handle:    handle,
		DataIndex: dataIndex,
	}, handle
}

func (f *FromDriver) GetDataIndex() int {
	return f.DataIndex
}

type FromNode struct {
	FileHash  cdssdk.FileHash
	Hub       cdssdk.Hub
	Storage   cdssdk.Storage
	DataIndex int
}

func NewFromStorage(fileHash cdssdk.FileHash, hub cdssdk.Hub, storage cdssdk.Storage, dataIndex int) *FromNode {
	return &FromNode{
		FileHash:  fileHash,
		Hub:       hub,
		DataIndex: dataIndex,
	}
}

func (f *FromNode) GetDataIndex() int {
	return f.DataIndex
}

type ToDriver struct {
	Handle    *exec.DriverReadStream
	DataIndex int
	Range     math2.Range
}

func NewToDriver(dataIndex int) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:    &str,
		DataIndex: dataIndex,
	}, &str
}

func NewToDriverWithRange(dataIndex int, rng math2.Range) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:    &str,
		DataIndex: dataIndex,
		Range:     rng,
	}, &str
}

func (t *ToDriver) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToDriver) GetRange() math2.Range {
	return t.Range
}

type ToNode struct {
	Hub              cdssdk.Hub
	Storage          cdssdk.Storage
	DataIndex        int
	Range            math2.Range
	FileHashStoreKey string
}

func NewToStorage(hub cdssdk.Hub, stg cdssdk.Storage, dataIndex int, fileHashStoreKey string) *ToNode {
	return &ToNode{
		Hub:              hub,
		Storage:          stg,
		DataIndex:        dataIndex,
		FileHashStoreKey: fileHashStoreKey,
	}
}

func NewToStorageWithRange(hub cdssdk.Hub, stg cdssdk.Storage, dataIndex int, fileHashStoreKey string, rng math2.Range) *ToNode {
	return &ToNode{
		Hub:              hub,
		Storage:          stg,
		DataIndex:        dataIndex,
		FileHashStoreKey: fileHashStoreKey,
		Range:            rng,
	}
}

func (t *ToNode) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToNode) GetRange() math2.Range {
	return t.Range
}

// type ToStorage struct {
// 	Storage   cdssdk.Storage
// 	DataIndex int
// }

// func NewToStorage(storage cdssdk.Storage, dataIndex int) *ToStorage {
// 	return &ToStorage{
// 		Storage:   storage,
// 		DataIndex: dataIndex,
// 	}
// }

// func (t *ToStorage) GetDataIndex() int {
// 	return t.DataIndex
// }
