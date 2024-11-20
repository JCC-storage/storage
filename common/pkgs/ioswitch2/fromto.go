package ioswitch2

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type From interface {
	GetStreamType() StreamType
}

type To interface {
	// To所需要的文件流的范围。具体含义与DataIndex有关系：
	// 如果DataIndex == -1，则表示在整个文件的范围。
	// 如果DataIndex >= 0，则表示在文件的某个分片的范围。
	GetRange() exec.Range
	GetStreamType() StreamType
}

const (
	// 未处理的完整文件流
	StreamTypeRaw = iota
	// EC编码的某一块的流
	StreamTypeEC
	// 分段编码的某一段的流
	StreamTypeSegment
)

type StreamType struct {
	Type  int
	Index int
}

func RawStream() StreamType {
	return StreamType{
		Type: StreamTypeRaw,
	}
}

func ECSrteam(index int) StreamType {
	return StreamType{
		Type:  StreamTypeEC,
		Index: index,
	}
}

func SegmentStream(index int) StreamType {
	return StreamType{
		Type:  StreamTypeSegment,
		Index: index,
	}
}

func (s StreamType) IsRaw() bool {
	return s.Type == StreamTypeRaw
}

func (s StreamType) IsEC() bool {
	return s.Type == StreamTypeEC
}

func (s StreamType) IsSegment() bool {
	return s.Type == StreamTypeSegment
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
	Handle     *exec.DriverWriteStream
	StreamType StreamType
}

func NewFromDriver(strType StreamType) (*FromDriver, *exec.DriverWriteStream) {
	handle := &exec.DriverWriteStream{
		RangeHint: &exec.Range{},
	}
	return &FromDriver{
		Handle:     handle,
		StreamType: strType,
	}, handle
}

func (f *FromDriver) GetStreamType() StreamType {
	return f.StreamType
}

type FromShardstore struct {
	FileHash   cdssdk.FileHash
	Hub        cdssdk.Hub
	Storage    cdssdk.Storage
	StreamType StreamType
}

func NewFromShardstore(fileHash cdssdk.FileHash, hub cdssdk.Hub, storage cdssdk.Storage, strType StreamType) *FromShardstore {
	return &FromShardstore{
		FileHash:   fileHash,
		Hub:        hub,
		Storage:    storage,
		StreamType: strType,
	}
}

func (f *FromShardstore) GetStreamType() StreamType {
	return f.StreamType
}

type ToDriver struct {
	Handle     *exec.DriverReadStream
	StreamType StreamType
	Range      exec.Range
}

func NewToDriver(strType StreamType) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:     &str,
		StreamType: strType,
	}, &str
}

func NewToDriverWithRange(strType StreamType, rng exec.Range) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:     &str,
		StreamType: strType,
		Range:      rng,
	}, &str
}

func (t *ToDriver) GetStreamType() StreamType {
	return t.StreamType
}

func (t *ToDriver) GetRange() exec.Range {
	return t.Range
}

type ToShardStore struct {
	Hub              cdssdk.Hub
	Storage          cdssdk.Storage
	StreamType       StreamType
	Range            exec.Range
	FileHashStoreKey string
}

func NewToShardStore(hub cdssdk.Hub, stg cdssdk.Storage, strType StreamType, fileHashStoreKey string) *ToShardStore {
	return &ToShardStore{
		Hub:              hub,
		Storage:          stg,
		StreamType:       strType,
		FileHashStoreKey: fileHashStoreKey,
	}
}

func NewToShardStoreWithRange(hub cdssdk.Hub, stg cdssdk.Storage, streamType StreamType, fileHashStoreKey string, rng exec.Range) *ToShardStore {
	return &ToShardStore{
		Hub:              hub,
		Storage:          stg,
		StreamType:       streamType,
		FileHashStoreKey: fileHashStoreKey,
		Range:            rng,
	}
}

func (t *ToShardStore) GetStreamType() StreamType {
	return t.StreamType
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

func (t *LoadToShared) GetStreamType() StreamType {
	return StreamType{
		Type: StreamTypeRaw,
	}
}

func (t *LoadToShared) GetRange() exec.Range {
	return exec.Range{}
}
