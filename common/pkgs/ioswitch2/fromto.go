package ioswitch2

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type From interface {
	GetStreamIndex() StreamIndex
}

type To interface {
	// To所需要的文件流的范围。具体含义与DataIndex有关系：
	// 如果DataIndex == -1，则表示在整个文件的范围。
	// 如果DataIndex >= 0，则表示在文件的某个分片的范围。
	GetRange() exec.Range
	GetStreamIndex() StreamIndex
}

const (
	// 未处理的完整文件流
	StreamIndexRaw = iota
	// EC编码的某一块的流
	StreamIndexEC
	// 分段编码的某一段的流
	StreamIndexSegment
)

type StreamIndex struct {
	Type  int
	Index int
}

func RawStream() StreamIndex {
	return StreamIndex{
		Type: StreamIndexRaw,
	}
}

func ECSrteam(index int) StreamIndex {
	return StreamIndex{
		Type:  StreamIndexEC,
		Index: index,
	}
}

func SegmentStream(index int) StreamIndex {
	return StreamIndex{
		Type:  StreamIndexSegment,
		Index: index,
	}
}

func (s StreamIndex) IsRaw() bool {
	return s.Type == StreamIndexRaw
}

func (s StreamIndex) IsEC() bool {
	return s.Type == StreamIndexEC
}

func (s StreamIndex) IsSegment() bool {
	return s.Type == StreamIndexSegment
}

type FromTos []FromTo

type FromTo struct {
	// 如果输入或者输出用到了EC编码的流，则需要提供EC参数。
	ECParam *cdssdk.ECRedundancy
	// 同上
	SegmentParam *cdssdk.SegmentRedundancy
	Froms        []From
	Toes         []To
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
	Handle      *exec.DriverWriteStream
	StreamIndex StreamIndex
}

func NewFromDriver(strIdx StreamIndex) (*FromDriver, *exec.DriverWriteStream) {
	handle := &exec.DriverWriteStream{
		RangeHint: &exec.Range{},
	}
	return &FromDriver{
		Handle:      handle,
		StreamIndex: strIdx,
	}, handle
}

func (f *FromDriver) GetStreamIndex() StreamIndex {
	return f.StreamIndex
}

type FromShardstore struct {
	FileHash    cdssdk.FileHash
	Hub         cdssdk.Hub
	Storage     cdssdk.Storage
	StreamIndex StreamIndex
}

func NewFromShardstore(fileHash cdssdk.FileHash, hub cdssdk.Hub, storage cdssdk.Storage, strIdx StreamIndex) *FromShardstore {
	return &FromShardstore{
		FileHash:    fileHash,
		Hub:         hub,
		Storage:     storage,
		StreamIndex: strIdx,
	}
}

func (f *FromShardstore) GetStreamIndex() StreamIndex {
	return f.StreamIndex
}

type ToDriver struct {
	Handle      *exec.DriverReadStream
	StreamIndex StreamIndex
	Range       exec.Range
}

func NewToDriver(strIdx StreamIndex) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:      &str,
		StreamIndex: strIdx,
	}, &str
}

func NewToDriverWithRange(strIdx StreamIndex, rng exec.Range) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:      &str,
		StreamIndex: strIdx,
		Range:       rng,
	}, &str
}

func (t *ToDriver) GetStreamIndex() StreamIndex {
	return t.StreamIndex
}

func (t *ToDriver) GetRange() exec.Range {
	return t.Range
}

type ToShardStore struct {
	Hub              cdssdk.Hub
	Storage          stgmod.StorageDetail
	StreamIndex      StreamIndex
	Range            exec.Range
	FileHashStoreKey string
}

func NewToShardStore(hub cdssdk.Hub, stg stgmod.StorageDetail, strIdx StreamIndex, fileHashStoreKey string) *ToShardStore {
	return &ToShardStore{
		Hub:              hub,
		Storage:          stg,
		StreamIndex:      strIdx,
		FileHashStoreKey: fileHashStoreKey,
	}
}

func NewToShardStoreWithRange(hub cdssdk.Hub, stg stgmod.StorageDetail, streamIndex StreamIndex, fileHashStoreKey string, rng exec.Range) *ToShardStore {
	return &ToShardStore{
		Hub:              hub,
		Storage:          stg,
		StreamIndex:      streamIndex,
		FileHashStoreKey: fileHashStoreKey,
		Range:            rng,
	}
}

func (t *ToShardStore) GetStreamIndex() StreamIndex {
	return t.StreamIndex
}

func (t *ToShardStore) GetRange() exec.Range {
	return t.Range
}

type LoadToShared struct {
	Hub       cdssdk.Hub
	Storage   cdssdk.Storage
	UserID    cdssdk.UserID
	PackageID cdssdk.PackageID
	Path      string
}

func NewLoadToShared(hub cdssdk.Hub, storage cdssdk.Storage, userID cdssdk.UserID, packageID cdssdk.PackageID, path string) *LoadToShared {
	return &LoadToShared{
		Hub:       hub,
		Storage:   storage,
		UserID:    userID,
		PackageID: packageID,
		Path:      path,
	}
}

func (t *LoadToShared) GetStreamIndex() StreamIndex {
	return StreamIndex{
		Type: StreamIndexRaw,
	}
}

func (t *LoadToShared) GetRange() exec.Range {
	return exec.Range{}
}
