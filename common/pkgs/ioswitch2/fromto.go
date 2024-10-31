package ioswitch2

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type From interface {
	GetDataIndex() int
}

type To interface {
	// To所需要的文件流的范围。具体含义与DataIndex有关系：
	// 如果DataIndex == -1，则表示在整个文件的范围。
	// 如果DataIndex >= 0，则表示在文件的某个分片的范围。
	GetRange() exec.Range
	GetDataIndex() int
}

type FromTos []FromTo

type FromTo struct {
	Froms []From
	Toes  []To
}

func NewFromTo() FromTo {
	return FromTo{}
}

func (ft *FromTo) AddFrom(from From) *FromTo {
	ft.Froms = append(ft.Froms, from)
	return ft
}

func (ft *FromTo) AddTo(to To) *FromTo {
	ft.Toes = append(ft.Toes, to)
	return ft
}

type FromDriver struct {
	Handle    *exec.DriverWriteStream
	DataIndex int
}

func NewFromDriver(dataIndex int) (*FromDriver, *exec.DriverWriteStream) {
	handle := &exec.DriverWriteStream{
		RangeHint: &exec.Range{},
	}
	return &FromDriver{
		Handle:    handle,
		DataIndex: dataIndex,
	}, handle
}

func (f *FromDriver) GetDataIndex() int {
	return f.DataIndex
}

type FromShardstore struct {
	FileHash  cdssdk.FileHash
	Hub       cdssdk.Node
	Storage   cdssdk.Storage
	DataIndex int
}

func NewFromShardstore(fileHash cdssdk.FileHash, hub cdssdk.Node, storage cdssdk.Storage, dataIndex int) *FromShardstore {
	return &FromShardstore{
		FileHash:  fileHash,
		Hub:       hub,
		DataIndex: dataIndex,
	}
}

func (f *FromShardstore) GetDataIndex() int {
	return f.DataIndex
}

type ToDriver struct {
	Handle    *exec.DriverReadStream
	DataIndex int
	Range     exec.Range
}

func NewToDriver(dataIndex int) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:    &str,
		DataIndex: dataIndex,
	}, &str
}

func NewToDriverWithRange(dataIndex int, rng exec.Range) (*ToDriver, *exec.DriverReadStream) {
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

func (t *ToDriver) GetRange() exec.Range {
	return t.Range
}

type ToShardStore struct {
	Hub              cdssdk.Node
	Storage          cdssdk.Storage
	DataIndex        int
	Range            exec.Range
	FileHashStoreKey string
}

func NewToShardStore(hub cdssdk.Node, stg cdssdk.Storage, dataIndex int, fileHashStoreKey string) *ToShardStore {
	return &ToShardStore{
		Hub:              hub,
		Storage:          stg,
		DataIndex:        dataIndex,
		FileHashStoreKey: fileHashStoreKey,
	}
}

func NewToShardStoreWithRange(hub cdssdk.Node, stg cdssdk.Storage, dataIndex int, fileHashStoreKey string, rng exec.Range) *ToShardStore {
	return &ToShardStore{
		Hub:              hub,
		Storage:          stg,
		DataIndex:        dataIndex,
		FileHashStoreKey: fileHashStoreKey,
		Range:            rng,
	}
}

func (t *ToShardStore) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToShardStore) GetRange() exec.Range {
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
