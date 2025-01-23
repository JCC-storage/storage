package stgmod

import (
	"gitlink.org.cn/cloudream/common/pkgs/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"time"
)

type Source interface {
}

type Body interface {
}

var _ = serder.UseTypeUnionInternallyTagged(types.Ref(types.NewTypeUnion[Source](
	(*SourceCoordinator)(nil),
	(*SourceScanner)(nil),
)), "type")

type SourceCoordinator struct {
	serder.Metadata `union:"SourceCoordinator"`
	Type            string `json:"type"`
}

type SourceScanner struct {
	serder.Metadata `union:"SourceScanner"`
	Type            string       `json:"type"`
	HubID           cdssdk.HubID `json:"hubID"`
	HubName         string       `json:"hubName"`
}

var _ = serder.UseTypeUnionInternallyTagged(types.Ref(types.NewTypeUnion[Body](
	(*BodyHubInfo)(nil),
	(*BodyStorageInfo)(nil),
	(*BodyStorageStats)(nil),
	(*BodyHubTransferStats)(nil),
	(*BodyHubStorageTransferStats)(nil),
	(*BodyBlockTransfer)(nil),
	(*BodyBlockDistribution)(nil),
	(*BodyObjectChange)(nil),
	(*BodyPackageChange)(nil),
	(*BodyBucketChange)(nil),
)), "type")

type BodyHubInfo struct {
	serder.Metadata `union:"BodyHubInfo"`
	HubID           cdssdk.HubID `json:"hubID"`
	HubInfo         cdssdk.Hub   `json:"hubInfo"`
	Type            string       `json:"type"`
}

type BodyStorageInfo struct {
	serder.Metadata `union:"BodyStorageInfo"`
	StorageID       cdssdk.StorageID `json:"storageID"`
	StorageInfo     cdssdk.Storage   `json:"storageInfo"`
	Type            string           `json:"type"`
}
type BodyStorageStats struct {
	serder.Metadata `union:"BodyStorageStats"`
	StorageID       cdssdk.StorageID `json:"storageID"`
	DataCount       int64            `json:"dataCount"`
}

type DataTrans struct {
	TotalTransfer      int64 `json:"totalTransfer"`
	RequestCount       int64 `json:"requestCount"`
	FailedRequestCount int64 `json:"failedRequestCount"`
	AvgTransfer        int64 `json:"avgTransfer"`
	MaxTransfer        int64 `json:"maxTransfer"`
	MinTransfer        int64 `json:"minTransfer"`
}

type BodyHubTransferStats struct {
	serder.Metadata `union:"BodyHubTransferStats"`
	SourceHubID     cdssdk.HubID `json:"sourceHubID"`
	TargetHubID     cdssdk.HubID `json:"targetHubID"`
	Send            DataTrans    `json:"send"`
	StartTimestamp  time.Time    `json:"startTimestamp"`
	EndTimestamp    time.Time    `json:"endTimestamp"`
}

type BodyHubStorageTransferStats struct {
	serder.Metadata `union:"BodyHubStorageTransferStats"`
	HubID           cdssdk.HubID     `json:"hubID"`
	StorageID       cdssdk.StorageID `json:"storageID"`
	Send            DataTrans        `json:"send"`
	Receive         DataTrans        `json:"receive"`
	StartTimestamp  time.Time        `json:"startTimestamp"`
	EndTimestamp    time.Time        `json:"endTimestamp"`
}

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

type BlockChange struct {
	Type              string           `json:"type"`
	BlockType         string           `json:"blockType"`
	Index             string           `json:"index"`
	SourceStorageID   cdssdk.StorageID `json:"sourceStorageID"`
	TargetStorageID   cdssdk.StorageID `json:"targetStorageID"`
	DataTransferCount string           `json:"dataTransferCount"`
	Timestamp         time.Time        `json:"timestamp"`
	SourceBlocks      []Block          `json:"sourceBlocks,omitempty"`
	TargetBlocks      []Block          `json:"targetBlocks,omitempty"`
	DataTransfers     []DataTransfer   `json:"dataTransfers,omitempty"`
	Blocks            []Block          `json:"blocks,omitempty"`
}
type BodyBlockTransfer struct {
	serder.Metadata `union:"BodyBlockTransfer"`
	Type            string           `json:"type"`
	ObjectID        cdssdk.ObjectID  `json:"objectID"`
	PackageID       cdssdk.PackageID `json:"packageID"`
	BlockChanges    []BlockChange    `json:"blockChanges"`
}

type BlockDistributionObjectInfo struct {
	Type      string `json:"type"`
	Index     string `json:"index"`
	StorageID string `json:"storageID"`
}

type BlockDistributionObject struct {
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

type BodyBlockDistribution struct {
	serder.Metadata `union:"BodyBlockDistribution"`
	Timestamp       time.Time               `json:"timestamp"`
	Object          BlockDistributionObject `json:"object,omitempty"`
}
type BodyObjectChange struct {
	serder.Metadata   `union:"BodyObjectChange"`
	Type              string                        `json:"type"`
	ObjectID          cdssdk.ObjectID               `json:"objectID"`
	PackageID         cdssdk.PackageID              `json:"packageID"`
	Path              string                        `json:"path"`
	Size              int                           `json:"size"`
	BlockDistribution []BlockDistributionObjectInfo `json:"blockDistribution"`
	Timestamp         time.Time                     `json:"timestamp"`
}
type BodyPackageChange struct {
	serder.Metadata `union:"BodyPackageChange"`
	Type            string           `json:"type"`
	PackageID       cdssdk.PackageID `json:"packageID"`
	PackageName     string           `json:"packageName"`
	BucketID        cdssdk.BucketID  `json:"bucketID"`
	Timestamp       time.Time        `json:"timestamp"`
}
type BodyBucketChange struct {
	serder.Metadata `union:"BodyBucketChange"`
	Type            string          `json:"type"`
	BucketID        cdssdk.BucketID `json:"bucketID"`
	BucketName      string          `json:"bucketName"`
	Timestamp       time.Time       `json:"timestamp"`
}
