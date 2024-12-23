package stgmod

import "time"

// HubStat 中心信息 每天一次，各节点统计自身当前的总数据量

type Source struct {
	Type    string `json:"type"`
	HubID   string `json:"hubID"`
	HubName string `json:"hubName"`
}
type HubStatBody struct {
	MasterHubID      int    `json:"masterHubID"`
	MasterHubAddress string `json:"masterHubAddress"`
	StorageID        int    `json:"storageID"`
	DataCount        int    `json:"dataCount"`
}
type HubStat struct {
	Timestamp time.Time   `json:"timestamp"`
	Source    Source      `json:"source"`
	Category  string      `json:"category"`
	Body      HubStatBody `json:"body"`
}

// HubTrans 节点传输信息
// 每天一次，各节点统计自身当天向外部各个节点传输的总数据量

type HubTransBody struct {
	SourceHubID        int64     `json:"sourceHubID"`
	TargetHubID        int64     `json:"targetHubID"`
	DataTransferCount  int64     `json:"dataTransferCount"`
	RequestCount       int64     `json:"requestCount"`
	FailedRequestCount int64     `json:"failedRequestCount"`
	AvgTransferCount   int64     `json:"avgTransferCount"`
	MaxTransferCount   int64     `json:"maxTransferCount"`
	MinTransferCount   int64     `json:"minTransferCount"`
	StartTimestamp     time.Time `json:"startTimestamp"`
	EndTimestamp       time.Time `json:"endTimestamp"`
}
type HubTrans struct {
	Timestamp time.Time    `json:"timestamp"`
	Source    Source       `json:"source"`
	Category  string       `json:"category"`
	Body      HubTransBody `json:"body"`
}

// BlockTransInfo 块传输信息
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
	Timestamp         string         `json:"timestamp"`
	SourceBlocks      []Block        `json:"sourceBlocks,omitempty"`
	TargetBlocks      []Block        `json:"targetBlocks,omitempty"`
	DataTransfers     []DataTransfer `json:"dataTransfers,omitempty"`
	Blocks            []Block        `json:"blocks,omitempty"`
}

type BlockTransInfoBody struct {
	Type         string        `json:"type"`
	ObjectID     string        `json:"objectID"`
	PackageID    string        `json:"packageID"`
	BlockChanges []BlockChange `json:"blockChanges"`
}

type BlockTransInfo struct {
	Timestamp time.Time          `json:"timestamp"`
	Source    Source             `json:"source"`
	Category  string             `json:"category"`
	Body      BlockTransInfoBody `json:"body"`
}

// BlockDistribution 块传输信息
// 每天一次，在调整完成后，将当天调整前后的布局情况一起推送
type BlockDistribution struct {
	Type      string `json:"type"`
	Index     string `json:"index"`
	StorageID string `json:"storageID"`
}

type Object struct {
	ObjectID          string              `json:"objectID"`
	PackageID         string              `json:"packageID"`
	Path              string              `json:"path"`
	Size              int64               `json:"size"`
	FileHash          string              `json:"fileHash"`
	FaultTolerance    string              `json:"faultTolerance"`
	Redundancy        string              `json:"redundancy"`
	AvgAccessCost     string              `json:"avgAccessCost"`
	BlockDistribution []BlockDistribution `json:"blockDistribution"`
	DataTransfers     []DataTransfer      `json:"dataTransfers"`
}
type BlockDistributionBody struct {
	Timestamp     time.Time      `json:"timestamp"`
	Type          string         `json:"type"`
	ObjectID      string         `json:"objectID"`
	PackageID     string         `json:"packageID"`
	SourceBlocks  []Block        `json:"sourceBlocks,omitempty"`
	TargetBlocks  []Block        `json:"targetBlocks,omitempty"`
	DataTransfers []DataTransfer `json:"dataTransfers,omitempty"`
	Blocks        []Block        `json:"blocks,omitempty"`
}

type BlockDistributionInfo struct {
	Timestamp time.Time             `json:"timestamp"`
	Source    Source                `json:"source"`
	Category  string                `json:"category"`
	Body      BlockDistributionBody `json:"body"`
}

// ObjectUpdateInfo Object变化信息

type ObjectUpdateInfoBody struct {
	Type              string              `json:"type"`
	ObjectID          string              `json:"objectID"`
	PackageID         string              `json:"packageID"`
	Path              string              `json:"path"`
	Size              int                 `json:"size"`
	BlockDistribution []BlockDistribution `json:"blockDistribution"`
	Timestamp         string              `json:"timestamp"`
}
type ObjectUpdateInfo struct {
	Timestamp time.Time            `json:"timestamp"`
	Source    Source               `json:"source"`
	Category  string               `json:"category"`
	Body      ObjectUpdateInfoBody `json:"body"`
}

// PackageUpdateInfo package变化信息

type PackageUpdateInfoBody struct {
	Type        string `json:"type"`
	PackageID   string `json:"packageID"`
	PackageName string `json:"packageName"`
	BucketID    string `json:"bucketID"`
	Timestamp   string `json:"timestamp"`
}
type PackageUpdateInfo struct {
	Timestamp time.Time             `json:"timestamp"`
	Source    Source                `json:"source"`
	Category  string                `json:"category"`
	Body      PackageUpdateInfoBody `json:"body"`
}

// BucketUpdateInfo bucket变化信息

type BucketUpdateInfoBody struct {
	Type       string `json:"type"`
	BucketID   string `json:"bucketID"`
	BucketName string `json:"bucketName"`
	Timestamp  string `json:"timestamp"`
}

type BucketUpdateInfo struct {
	Timestamp time.Time            `json:"timestamp"`
	Source    Source               `json:"source"`
	Category  string               `json:"category"`
	Body      BucketUpdateInfoBody `json:"body"`
}
