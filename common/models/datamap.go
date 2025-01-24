package stgmod

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

// 系统事件
type SysEvent struct {
	Timestamp time.Time      `json:"timestamp"`
	Source    SysEventSource `json:"source"`
	Category  string         `json:"category"`
	Body      SysEventBody   `json:"body"`
}

// 事件源
type SysEventSource interface {
	GetSourceType() string
}

var _ = serder.UseTypeUnionInternallyTagged(types.Ref(types.NewTypeUnion[SysEventSource](
	(*SourceCoordinator)(nil),
	(*SourceScanner)(nil),
	(*SourceHub)(nil),
)), "type")

type SourceCoordinator struct {
	serder.Metadata `union:"Coordinator"`
	Type            string `json:"type"`
}

func (s *SourceCoordinator) GetSourceType() string {
	return "Coordinator"
}

type SourceScanner struct {
	serder.Metadata `union:"Scanner"`
	Type            string `json:"type"`
}

func (s *SourceScanner) GetSourceType() string {
	return "Scanner"
}

type SourceHub struct {
	serder.Metadata `union:"Hub"`
	Type            string       `json:"type"`
	HubID           cdssdk.HubID `json:"hubID"`
	HubName         string       `json:"hubName"`
}

func (s *SourceHub) GetSourceType() string {
	return "Hub"
}

// 事件体
type SysEventBody interface {
	GetBodyType() string
}

var _ = serder.UseTypeUnionInternallyTagged(types.Ref(types.NewTypeUnion[SysEventBody](
	(*BodyNewHub)(nil),
	(*BodyHubUpdated)(nil),
	(*BodyHubDeleted)(nil),

	(*BodyNewStorage)(nil),
	(*BodyStorageUpdated)(nil),
	(*BodyStorageDeleted)(nil),

	(*BodyStorageStats)(nil),
	(*BodyHubTransferStats)(nil),
	(*BodyHubStorageTransferStats)(nil),
	(*BodyBlockTransfer)(nil),
	(*BodyBlockDistribution)(nil),

	(*BodyNewObject)(nil),
	(*BodyObjectUpdated)(nil),
	(*BodyObjectDeleted)(nil),

	(*BodyNewPackage)(nil),
	(*BodyPackageDeleted)(nil),

	(*BodyNewBucket)(nil),
	(*BodyBucketDeleted)(nil),
)), "type")

// 新增Hub的事件
type BodyNewHub struct {
	serder.Metadata `union:"NewHub"`
	Type            string     `json:"type"`
	Info            cdssdk.Hub `json:"info"`
}

func (b *BodyNewHub) GetBodyType() string {
	return "NewHub"
}

// Hub信息更新的事件
type BodyHubUpdated struct {
	serder.Metadata `union:"HubUpdated"`
	Type            string     `json:"type"`
	Info            cdssdk.Hub `json:"info"`
}

func (b *BodyHubUpdated) GetBodyType() string {
	return "HubUpdated"
}

// Hub删除的事件
type BodyHubDeleted struct {
	serder.Metadata `union:"HubDeleted"`
	Type            string       `json:"type"`
	HubID           cdssdk.HubID `json:"hubID"`
}

func (b *BodyHubDeleted) GetBodyType() string {
	return "HubDeleted"
}

// 新增Storage的事件
type BodyNewStorage struct {
	serder.Metadata `union:"NewStorage"`
	Info            cdssdk.Storage `json:"info"`
	Type            string         `json:"type"`
}

func (b *BodyNewStorage) GetBodyType() string {
	return "NewStorage"
}

// Storage信息更新的事件
type BodyStorageUpdated struct {
	serder.Metadata `union:"StorageUpdated"`
	Type            string         `json:"type"`
	Info            cdssdk.Storage `json:"info"`
}

func (b *BodyStorageUpdated) GetBodyType() string {
	return "StorageUpdated"
}

// Storage删除的事件
type BodyStorageDeleted struct {
	serder.Metadata `union:"StorageDeleted"`
	Type            string           `json:"type"`
	StorageID       cdssdk.StorageID `json:"storageID"`
}

func (b *BodyStorageDeleted) GetBodyType() string {
	return "StorageDeleted"
}

// Storage统计信息的事件
type BodyStorageStats struct {
	serder.Metadata `union:"StorageStats"`
	Type            string           `json:"type"`
	StorageID       cdssdk.StorageID `json:"storageID"`
	DataCount       int64            `json:"dataCount"`
}

func (b *BodyStorageStats) GetBodyType() string {
	return "StorageStats"
}

// Hub数据传输统计信息的事件
type BodyHubTransferStats struct {
	serder.Metadata `union:"HubTransferStats"`
	Type            string       `json:"type"`
	SourceHubID     cdssdk.HubID `json:"sourceHubID"`
	TargetHubID     cdssdk.HubID `json:"targetHubID"`
	Send            DataTrans    `json:"send"`
	StartTimestamp  time.Time    `json:"startTimestamp"`
	EndTimestamp    time.Time    `json:"endTimestamp"`
}

func (b *BodyHubTransferStats) GetBodyType() string {
	return "HubTransferStats"
}

type DataTrans struct {
	TotalTransfer      int64 `json:"totalTransfer"`
	RequestCount       int64 `json:"requestCount"`
	FailedRequestCount int64 `json:"failedRequestCount"`
	AvgTransfer        int64 `json:"avgTransfer"`
	MaxTransfer        int64 `json:"maxTransfer"`
	MinTransfer        int64 `json:"minTransfer"`
}

// Hub和Storage数据传输统计信息的事件
type BodyHubStorageTransferStats struct {
	serder.Metadata `union:"HubStorageTransferStats"`
	Type            string           `json:"type"`
	HubID           cdssdk.HubID     `json:"hubID"`
	StorageID       cdssdk.StorageID `json:"storageID"`
	Send            DataTrans        `json:"send"`
	Receive         DataTrans        `json:"receive"`
	StartTimestamp  time.Time        `json:"startTimestamp"`
	EndTimestamp    time.Time        `json:"endTimestamp"`
}

func (b *BodyHubStorageTransferStats) GetBodyType() string {
	return "HubStorageTransferStats"
}

// 块传输的事件
type BodyBlockTransfer struct {
	serder.Metadata `union:"BlockTransfer"`
	Type            string           `json:"type"`
	ObjectID        cdssdk.ObjectID  `json:"objectID"`
	PackageID       cdssdk.PackageID `json:"packageID"`
	BlockChanges    []BlockChange    `json:"blockChanges"`
}

func (b *BodyBlockTransfer) GetBodyType() string {
	return "BlockTransfer"
}

// 块变化类型
type BlockChange interface {
	GetBlockChangeType() string
}

var _ = serder.UseTypeUnionInternallyTagged(types.Ref(types.NewTypeUnion[BlockChange](
	(*BlockChangeClone)(nil),
	(*BlockChangeDeleted)(nil),
	(*BlockChangeEnDecode)(nil),
	// (*BlockChangeUpdated)(nil),
)), "type")

type Block struct {
	BlockType string           `json:"blockType"`
	Index     string           `json:"index"`
	StorageID cdssdk.StorageID `json:"storageID"`
}
type DataTransfer struct {
	SourceStorageID   cdssdk.StorageID `json:"sourceStorageID"`
	TargetStorageID   cdssdk.StorageID `json:"targetStorageID"`
	DataTransferCount string           `json:"dataTransferCount"`
}

type BlockChangeClone struct {
	serder.Metadata   `union:"BlockChangeClone"`
	Type              string           `json:"type"`
	BlockType         string           `json:"blockType"`
	Index             string           `json:"index"`
	SourceStorageID   cdssdk.StorageID `json:"sourceStorageID"`
	TargetStorageID   cdssdk.StorageID `json:"targetStorageID"`
	DataTransferCount string           `json:"dataTransferCount"`
}

func (b *BlockChangeClone) GetBlockChangeType() string {
	return "Clone"
}

type BlockChangeDeleted struct {
	serder.Metadata `union:"BlockChangeDeleted"`
	Type            string `json:"type"`
	Index           string `json:"index"`
	StorageID       string `json:"storageID"`
}

func (b *BlockChangeDeleted) GetBlockChangeType() string {
	return "Deleted"
}

type BlockChangeEnDecode struct {
	serder.Metadata `union:"BlockChangeEnDecode"`
	Type            string         `json:"type"`
	SourceBlocks    []Block        `json:"sourceBlocks,omitempty"`
	TargetBlocks    []Block        `json:"targetBlocks,omitempty"`
	DataTransfers   []DataTransfer `json:"dataTransfers,omitempty"`
}

func (b *BlockChangeEnDecode) GetBlockChangeType() string {
	return "EnDecode"
}

// TODO 块更新应该是说对象被重新上传，此时事件内应该包含全部对象块的信息，因此应该使用ObjectUpdated事件
// type BlockChangeUpdated struct {
// 	serder.Metadata `union:"BlockChangeUpdated"`
// 	Type            string  `json:"type"`
// 	Blocks          []Block `json:"blocks"`
// }

// func (b *BlockChangeUpdated) GetBlockChangeType() string {
// 	return "Updated"
// }

// 块分布的事件
type BodyBlockDistribution struct {
	serder.Metadata   `union:"BlockDistribution"`
	Type              string                        `json:"type"`
	ObjectID          cdssdk.ObjectID               `json:"objectID"`
	PackageID         cdssdk.PackageID              `json:"packageID"`
	Path              string                        `json:"path"`
	Size              int64                         `json:"size"`
	FileHash          string                        `json:"fileHash"`
	FaultTolerance    string                        `json:"faultTolerance"`
	Redundancy        string                        `json:"redundancy"`
	AvgAccessCost     string                        `json:"avgAccessCost"`
	BlockDistribution []BlockDistributionObjectInfo `json:"blockDistribution"`
	DataTransfers     []DataTransfer                `json:"dataTransfers"`
}

func (b *BodyBlockDistribution) GetBodyType() string {
	return "BlockDistribution"
}

type BlockDistributionObjectInfo struct {
	Type      string `json:"type"`
	Index     string `json:"index"`
	StorageID string `json:"storageID"`
}

// 新增Object的事件
type BodyNewObject struct {
	serder.Metadata   `union:"NewObject"`
	Type              string                        `json:"type"`
	Info              cdssdk.Object                 `json:"info"`
	BlockDistribution []BlockDistributionObjectInfo `json:"blockDistribution"`
}

func (b *BodyNewObject) GetBodyType() string {
	return "NewObject"
}

// Object更新的事件
type BodyObjectUpdated struct {
	serder.Metadata   `union:"ObjectUpdated"`
	Type              string                        `json:"type"`
	Info              cdssdk.Object                 `json:"info"`
	BlockDistribution []BlockDistributionObjectInfo `json:"blockDistribution"`
}

func (b *BodyObjectUpdated) GetBodyType() string {
	return "ObjectUpdated"
}

// Object删除的事件
type BodyObjectDeleted struct {
	serder.Metadata `union:"ObjectDeleted"`
	Type            string          `json:"type"`
	ObjectID        cdssdk.ObjectID `json:"objectID"`
}

func (b *BodyObjectDeleted) GetBodyType() string {
	return "ObjectDeleted"
}

// 新增Package的事件
type BodyNewPackage struct {
	serder.Metadata `union:"NewPackage"`
	Type            string         `json:"type"`
	Info            cdssdk.Package `json:"info"`
}

func (b *BodyNewPackage) GetBodyType() string {
	return "NewPackage"
}

// Package删除的事件
type BodyPackageDeleted struct {
	serder.Metadata `union:"PackageDeleted"`
	Type            string           `json:"type"`
	PackageID       cdssdk.PackageID `json:"packageID"`
}

func (b *BodyPackageDeleted) GetBodyType() string {
	return "PackageDeleted"
}

// 新增Bucket的事件
type BodyNewBucket struct {
	serder.Metadata `union:"NewBucket"`
	Type            string        `json:"type"`
	Info            cdssdk.Bucket `json:"info"`
}

func (b *BodyNewBucket) GetBodyType() string {
	return "NewBucket"
}

// Bucket删除的事件
type BodyBucketDeleted struct {
	serder.Metadata `union:"BucketDeleted"`
	Type            string          `json:"type"`
	BucketID        cdssdk.BucketID `json:"bucketID"`
}

func (b *BodyBucketDeleted) GetBodyType() string {
	return "BucketDeleted"
}
