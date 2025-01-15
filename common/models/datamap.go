package stgmod

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"time"
)

// HubInfo 节点信息

type SourceHub struct {
	Type string `json:"type"`
}

type HubInfoBody struct {
	HubID   int        `json:"hubID"`
	HubInfo cdssdk.Hub `json:"hubInfo"`
	Type    string     `json:"type"`
}

type HubInfo struct {
	Timestamp time.Time   `json:"timestamp"`
	Source    SourceHub   `json:"source"`
	Category  string      `json:"category"`
	Body      HubInfoBody `json:"body"`
}

//StorageInfo 节点信息

type SourceStorage struct {
	Type string `json:"type"`
}

type StorageInfoBody struct {
	StorageID   int64          `json:"storageID"`
	StorageInfo cdssdk.Storage `json:"storageInfo"`
	Type        string         `json:"type"`
}

type StorageInfo struct {
	Timestamp time.Time       `json:"timestamp"`
	Source    SourceStorage   `json:"source"`
	Category  string          `json:"category"`
	Body      StorageInfoBody `json:"body"`
}

// StorageStats 节点信息数据

type Source struct {
	Type    string `json:"type"`
	HubID   string `json:"hubID"`
	HubName string `json:"hubName"`
}

type StorageStatsBody struct {
	StorageID int64 `json:"storageID"`
	DataCount int64 `json:"dataCount"`
}

type StorageStats struct {
	Timestamp time.Time        `json:"timestamp"`
	Source    Source           `json:"source"`
	Category  string           `json:"category"`
	Body      StorageStatsBody `json:"body"`
}

// HubTransferStats 节点传输信息
// 每天一次，各节点统计自身当天向外部各个节点传输的总数据量

type DataTrans struct {
	TotalTransfer      int64 `json:"totalTransfer"`
	RequestCount       int64 `json:"requestCount"`
	FailedRequestCount int64 `json:"failedRequestCount"`
	AvgTransfer        int64 `json:"avgTransfer"`
	MaxTransfer        int64 `json:"maxTransfer"`
	MinTransfer        int64 `json:"minTransfer"`
}
type HubTransferStatsBody struct {
	SourceHubID    int64     `json:"sourceHubID"`
	TargetHubID    int64     `json:"targetHubID"`
	Send           DataTrans `json:"send"`
	StartTimestamp time.Time `json:"startTimestamp"`
	EndTimestamp   time.Time `json:"endTimestamp"`
}
type HubTransferStats struct {
	Timestamp time.Time            `json:"timestamp"`
	Source    Source               `json:"source"`
	Category  string               `json:"category"`
	Body      HubTransferStatsBody `json:"body"`
}

//hubStorageTransferStats 节点-中心间传输信息

type HubStorageTransferStatsBody struct {
	HubID          int64     `json:"hubID"`
	StorageID      int64     `json:"storageID"`
	Send           DataTrans `json:"send"`
	Receive        DataTrans `json:"receive"`
	StartTimestamp time.Time `json:"startTimestamp"`
	EndTimestamp   time.Time `json:"endTimestamp"`
}

type HubStorageTransferStats struct {
	Timestamp time.Time                   `json:"timestamp"`
	Source    Source                      `json:"source"`
	Category  string                      `json:"category"`
	Body      HubStorageTransferStatsBody `json:"body"`
}

// blockTransfer 块传输信息
/*实时日志，当对象的块信息发生变化时推送，共有4种变化类型：
拷贝
编解码(一变多、多变一、多变多)
删除
更新*/

type Block struct {
	BlockType string `json:"blockType"`
	Index     string `json:"index"`
	StorageID string `json:"storageID"`
}
type DataTransfer struct {
	SourceStorageID   string `json:"sourceStorageID"`
	TargetStorageID   string `json:"targetStorageID"`
	DataTransferCount string `json:"dataTransferCount"`
}

type BlockChange struct {
	Type              string         `json:"type"`
	BlockType         string         `json:"blockType"`
	Index             string         `json:"index"`
	SourceStorageID   string         `json:"sourceStorageID"`
	TargetStorageID   string         `json:"targetStorageID"`
	DataTransferCount string         `json:"dataTransferCount"`
	Timestamp         time.Time      `json:"timestamp"`
	SourceBlocks      []Block        `json:"sourceBlocks,omitempty"`
	TargetBlocks      []Block        `json:"targetBlocks,omitempty"`
	DataTransfers     []DataTransfer `json:"dataTransfers,omitempty"`
	Blocks            []Block        `json:"blocks,omitempty"`
}

type BlockTransferBody struct {
	Type         string        `json:"type"`
	ObjectID     string        `json:"objectID"`
	PackageID    string        `json:"packageID"`
	BlockChanges []BlockChange `json:"blockChanges"`
}

type BlockTransfer struct {
	Timestamp time.Time         `json:"timestamp"`
	Source    Source            `json:"source"`
	Category  string            `json:"category"`
	Body      BlockTransferBody `json:"body"`
}

// BlockDistribution 块传输信息
// 每天一次，在调整完成后，将当天调整前后的布局情况一起推送

type BlockDistributionObjectInfo struct {
	Type      string `json:"type"`
	Index     string `json:"index"`
	StorageID string `json:"storageID"`
}

type BlockDistributionObject struct {
	ObjectID          int64                         `json:"objectID"`
	PackageID         int64                         `json:"packageID"`
	Path              string                        `json:"path"`
	Size              int64                         `json:"size"`
	FileHash          string                        `json:"fileHash"`
	FaultTolerance    string                        `json:"faultTolerance"`
	Redundancy        string                        `json:"redundancy"`
	AvgAccessCost     string                        `json:"avgAccessCost"`
	BlockDistribution []BlockDistributionObjectInfo `json:"blockDistribution"`
	DataTransfers     []DataTransfer                `json:"dataTransfers"`
}
type BlockDistributionBody struct {
	Timestamp time.Time               `json:"timestamp"`
	Object    BlockDistributionObject `json:"object,omitempty"`
}

type BlockDistribution struct {
	Timestamp time.Time             `json:"timestamp"`
	Source    Source                `json:"source"`
	Category  string                `json:"category"`
	Body      BlockDistributionBody `json:"body"`
}

// ObjectChange Object变化信息

type ObjectChangeBody struct {
	Type              string              `json:"type"`
	ObjectID          string              `json:"objectID"`
	PackageID         string              `json:"packageID"`
	Path              string              `json:"path"`
	Size              int                 `json:"size"`
	BlockDistribution []BlockDistribution `json:"blockDistribution"`
	Timestamp         string              `json:"timestamp"`
}
type ObjectChange struct {
	Timestamp time.Time        `json:"timestamp"`
	Source    Source           `json:"source"`
	Category  string           `json:"category"`
	Body      ObjectChangeBody `json:"body"`
}

// PackageChange package变化信息

type PackageChangeBody struct {
	Type        string `json:"type"`
	PackageID   string `json:"packageID"`
	PackageName string `json:"packageName"`
	BucketID    string `json:"bucketID"`
	Timestamp   string `json:"timestamp"`
}
type PackageChange struct {
	Timestamp time.Time         `json:"timestamp"`
	Source    Source            `json:"source"`
	Category  string            `json:"category"`
	Body      PackageChangeBody `json:"body"`
}

// BucketChange bucket变化信息

type BucketChangeBody struct {
	Type       string `json:"type"`
	BucketID   string `json:"bucketID"`
	BucketName string `json:"bucketName"`
	Timestamp  string `json:"timestamp"`
}

type BucketChange struct {
	Timestamp time.Time        `json:"timestamp"`
	Source    Source           `json:"source"`
	Category  string           `json:"category"`
	Body      BucketChangeBody `json:"body"`
}
