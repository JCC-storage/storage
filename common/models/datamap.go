package stgmod

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

// 系统事件
type SysEvent struct {
	Timestamp time.Time      `json:"timestamp"`
	Source    SysEventSource `json:"source"`
	Body      SysEventBody   `json:"body"`
}

func (e *SysEvent) String() string {
	return fmt.Sprintf("%v [%v] %+v", e.Timestamp.Format("2006-01-02 15:04:05"), e.Source, e.Body)
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

func (s *SourceCoordinator) OnUnionSerializing() {
	s.Type = s.GetSourceType()
}

func (s *SourceCoordinator) String() string {
	return "Coordinator"
}

type SourceScanner struct {
	serder.Metadata `union:"Scanner"`
	Type            string `json:"type"`
}

func (s *SourceScanner) GetSourceType() string {
	return "Scanner"
}

func (s *SourceScanner) OnUnionSerializing() {
	s.Type = s.GetSourceType()
}

func (s *SourceScanner) String() string {
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

func (s *SourceHub) OnUnionSerializing() {
	s.Type = s.GetSourceType()
}

func (s *SourceHub) String() string {
	return fmt.Sprintf("Hub(%d, %s)", s.HubID, s.HubName)
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

	(*BodyNewOrUpdateObject)(nil),
	(*BodyObjectInfoUpdated)(nil),
	(*BodyObjectDeleted)(nil),

	(*BodyNewPackage)(nil),
	(*BodyPackageCloned)(nil),
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

func (b *BodyNewHub) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyHubUpdated) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyHubDeleted) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyNewStorage) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyStorageUpdated) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyStorageDeleted) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyStorageStats) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyHubTransferStats) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyHubStorageTransferStats) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyBlockTransfer) OnUnionSerializing() {
	b.Type = b.GetBodyType()
}

// 块变化类型
type BlockChange interface {
	GetBlockChangeType() string
}

var _ = serder.UseTypeUnionInternallyTagged(types.Ref(types.NewTypeUnion[BlockChange](
	(*BlockChangeClone)(nil),
	(*BlockChangeDeleted)(nil),
	(*BlockChangeEnDecode)(nil),
)), "type")

const (
	BlockTypeRaw     = "Raw"
	BlockTypeEC      = "EC"
	BlockTypeSegment = "Segment"
)

type Block struct {
	BlockType string           `json:"blockType"`
	Index     int              `json:"index"`
	StorageID cdssdk.StorageID `json:"storageID"`
}
type DataTransfer struct {
	SourceStorageID cdssdk.StorageID `json:"sourceStorageID"`
	TargetStorageID cdssdk.StorageID `json:"targetStorageID"`
	TransferBytes   int64            `json:"transferBytes"`
}

type BlockChangeClone struct {
	serder.Metadata `union:"Clone"`
	Type            string           `json:"type"`
	BlockType       string           `json:"blockType"`
	Index           int              `json:"index"`
	SourceStorageID cdssdk.StorageID `json:"sourceStorageID"`
	TargetStorageID cdssdk.StorageID `json:"targetStorageID"`
	TransferBytes   int64            `json:"transferBytes"`
}

func (b *BlockChangeClone) GetBlockChangeType() string {
	return "Clone"
}

func (b *BlockChangeClone) OnUnionSerializing() {
	b.Type = b.GetBlockChangeType()
}

type BlockChangeDeleted struct {
	serder.Metadata `union:"Deleted"`
	Type            string           `json:"type"`
	Index           int              `json:"index"`
	StorageID       cdssdk.StorageID `json:"storageID"`
}

func (b *BlockChangeDeleted) GetBlockChangeType() string {
	return "Deleted"
}

func (b *BlockChangeDeleted) OnUnionSerializing() {
	b.Type = b.GetBlockChangeType()
}

type BlockChangeEnDecode struct {
	serder.Metadata `union:"EnDecode"`
	Type            string         `json:"type"`
	SourceBlocks    []Block        `json:"sourceBlocks,omitempty"`
	TargetBlocks    []Block        `json:"targetBlocks,omitempty"`
	DataTransfers   []DataTransfer `json:"dataTransfers,omitempty"`
}

func (b *BlockChangeEnDecode) GetBlockChangeType() string {
	return "EnDecode"
}

func (b *BlockChangeEnDecode) OnUnionSerializing() {
	b.Type = b.GetBlockChangeType()
}

// 块分布的事件
type BodyBlockDistribution struct {
	serder.Metadata   `union:"BlockDistribution"`
	Type              string                        `json:"type"`
	ObjectID          cdssdk.ObjectID               `json:"objectID"`
	PackageID         cdssdk.PackageID              `json:"packageID"`
	Path              string                        `json:"path"`
	Size              int64                         `json:"size"`
	FileHash          cdssdk.FileHash               `json:"fileHash"`
	FaultTolerance    float64                       `json:"faultTolerance"`
	Redundancy        float64                       `json:"redundancy"`
	AvgAccessCost     float64                       `json:"avgAccessCost"`
	BlockDistribution []BlockDistributionObjectInfo `json:"blockDistribution"`
	DataTransfers     []DataTransfer                `json:"dataTransfers"`
}

func (b *BodyBlockDistribution) GetBodyType() string {
	return "BlockDistribution"
}

func (b *BodyBlockDistribution) OnUnionSerializing() {
	b.Type = b.GetBodyType()
}

type BlockDistributionObjectInfo struct {
	BlockType string           `json:"type"`
	Index     int              `json:"index"`
	StorageID cdssdk.StorageID `json:"storageID"`
}

// 新增或者重新上传Object的事件
type BodyNewOrUpdateObject struct {
	serder.Metadata   `union:"NewOrUpdateObject"`
	Type              string                        `json:"type"`
	Info              cdssdk.Object                 `json:"info"`
	BlockDistribution []BlockDistributionObjectInfo `json:"blockDistribution"`
}

func (b *BodyNewOrUpdateObject) GetBodyType() string {
	return "NewOrUpdateObject"
}

func (b *BodyNewOrUpdateObject) OnUnionSerializing() {
	b.Type = b.GetBodyType()
}

// Object的基本信息更新的事件
type BodyObjectInfoUpdated struct {
	serder.Metadata `union:"ObjectInfoUpdated"`
	Type            string        `json:"type"`
	Object          cdssdk.Object `json:"object"`
}

func (b *BodyObjectInfoUpdated) GetBodyType() string {
	return "ObjectInfoUpdated"
}

func (b *BodyObjectInfoUpdated) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyObjectDeleted) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyNewPackage) OnUnionSerializing() {
	b.Type = b.GetBodyType()
}

// Package克隆的事件
type BodyPackageCloned struct {
	serder.Metadata `union:"PackageCloned"`
	Type            string           `json:"type"`
	SourcePackageID cdssdk.PackageID `json:"sourcePackageID"`
	NewPackage      cdssdk.Package   `json:"newPackage"`
}

func (b *BodyPackageCloned) GetBodyType() string {
	return "PackageCloned"
}

func (b *BodyPackageCloned) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyPackageDeleted) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyNewBucket) OnUnionSerializing() {
	b.Type = b.GetBodyType()
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

func (b *BodyBucketDeleted) OnUnionSerializing() {
	b.Type = b.GetBodyType()
}
