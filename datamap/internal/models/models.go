package models

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type RequestID int64

type BlockID int64

type RelationshipID int64

type Status int

const (
	StatusNow                  Status = iota // =0  表示当前实时状态
	StatusYesterdayAfter                     // =1  表示前一天调整后的状态
	StatusYesterdayBefore                    // =2  表示前一天调整前的状态
	StatusTodayBeforeYesterday               // =3  表示前两天调整后的状态
)

// 节点间关系图
type HubRelationship struct {
	HubID      cdssdk.HubID          `json:"hubID"`      // 节点ID
	HubName    string                `json:"hubName"`    // 名称
	HubAddress cdssdk.HubAddressInfo `json:"hubAddress"` // 地址
	Storages   []StorageInfo         `json:"storages"`   // 对应的中心信息
	Requests   []Request             `json:"requests"`   // 与各节点的连接信息（仅记录发出的请求）
}

type StorageInfo struct {
	StorageID    cdssdk.StorageID `json:"storageID"`    // 中心ID
	DataCount    int64            `json:"dataCount"`    // 总数据量（文件分块数）
	NewDataCount int64            `json:"newdataCount"` // 新增数据量（文件分块数）
	Timestamp    time.Time        `json:"timestamp"`    // 时间戳
}

type Request struct {
	SourceHubID        cdssdk.HubID `json:"sourceHubID"`        // 源节点ID
	TargetHubID        cdssdk.HubID `json:"targetHubID"`        // 目标节点ID
	DataTransferCount  int64        `json:"dataTransferCount"`  // 数据传输量
	RequestCount       int64        `json:"requestCount"`       // 请求数
	FailedRequestCount int64        `json:"failedRequestCount"` // 失败请求数
	AvgTransferCount   int64        `json:"avgTransferCount"`   // 平均数据传输量
	MaxTransferCount   int64        `json:"maxTransferCount"`   // 最大数据传输量
	MinTransferCount   int64        `json:"minTransferCount"`   // 最小数据传输量
	StartTimestamp     time.Time    `json:"startTimestamp"`     // 起始时间戳
	EndTimestamp       time.Time    `json:"endTimestamp"`       // 结束时间戳
}

// 对象块分布结构
type ObjectDistribution struct {
	ObjectID      cdssdk.ObjectID  `json:"objectID"`      // 对象 ID
	PackageID     cdssdk.PackageID `json:"packageID"`     // 包 ID
	Path          string           `json:"path"`          // 路径
	Size          int64            `json:"size"`          // 大小
	FileHash      string           `json:"fileHash"`      // 文件哈希
	States        []State          `json:"states"`        // 各阶段状态信息（只需要传1、2、3阶段）
	Relationships []Relationship   `json:"relationships"` // 节点间传输量
	Timestamp     time.Time        `json:"timestamp"`     // 请求中的时间戳
}

type State struct {
	Timestamp          time.Time   `json:"timestamp"`          // 时间戳
	Status             string      `json:"status"`             // 状态
	FaultTolerance     string      `json:"faultTolerance"`     // 容灾度（仅布局调整后）
	Redundancy         string      `json:"redundancy"`         // 冗余度（仅布局调整后）
	AvgAccessCost      float64     `json:"avgAccessCost"`      // 平均访问开销（仅布局调整前）
	BlockDistributions []BlockDist `json:"blockDistributions"` // 块分布情况
}

type BlockDist struct {
	StorageID cdssdk.StorageID `json:"storageID"` // 中心ID
	Blocks    []Block          `json:"blocks"`    // 该中心的所有块
}

type Block struct {
	Type  string `json:"type"`  // 块类型
	Index string `json:"index"` // 块编号
	ID    string `json:"id"`    // 块ID
}

type Relationship struct {
	Status            Status    `json:"status"`            // 连线左侧的状态
	SourceStorageID   string    `json:"sourceStorageID"`   // 源存储节点 ID
	TargetStorageID   string    `json:"targetStorageID"`   // 目标存储节点 ID
	DataTransferCount string    `json:"dataTransferCount"` // 数据传输量
	Timestamp         time.Time `json:"timestamp"`         // 变化结束时间戳
}

//数据库结构定义

type Hub struct {
	HubID   cdssdk.HubID          `gorm:"column:HubID; primaryKey; type:bigint; autoIncrement" json:"hubID"`
	Name    string                `gorm:"column:Name; type:varchar(255); not null" json:"name"`
	Address cdssdk.HubAddressInfo `gorm:"column:Address; type:json; serializer:union" json:"address"`
}

func (Hub) TableName() string {
	return "Hub"
}

type Storage struct {
	StorageID    cdssdk.StorageID `gorm:"column:StorageID; primaryKey; type:bigint; autoIncrement" json:"storageID"`
	HubID        cdssdk.HubID     `gorm:"column:HubID; type:bigint; not null" json:"hubID"`
	DataCount    int64            `gorm:"column:DataCount; type:bigint; not null" json:"dataCount"`
	NewDataCount int64            `gorm:"column:NewDataCount; type:bigint; not null" json:"newDataCount"`
	Timestamp    time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}

func (Storage) TableName() string {
	return "Storage"
}

type HubRequest struct {
	RequestID        int64        `gorm:"column:HubID; primaryKey; type:bigint; autoIncrement" json:"hubID"`
	SourceHubID      cdssdk.HubID `gorm:"column:SourceHubID; type:bigint; not null" json:"sourceHubID"`
	TargetHubID      cdssdk.HubID `gorm:"column:TargetHubID; type:bigint; not null" json:"targetHubID"`
	DataTransfer     int64        `gorm:"column:DataTransfer; type:bigint; not null" json:"dataTransfer"`
	RequestCount     int64        `gorm:"column:RequestCount; type:bigint; not null" json:"requestCount"`
	FailedRequest    int64        `gorm:"column:FailedRequest; type:bigint; not null" json:"failedRequest"`
	AvgTransferCount int64        `gorm:"column:AvgTransferCount; type:bigint; not null" json:"avgTransferCount"`
	MaxTransferCount int64        `gorm:"column:MaxTransferCount; type:bigint; not null" json:"maxTransferCount"`
	MinTransferCount int64        `gorm:"column:MinTransferCount; type:bigint; not null" json:"minTransferCount"`
	StartTimestamp   time.Time    `gorm:"column:StartTimestamp; type:datatime; not null" json:"startTimestamp"`
	EndTimestamp     time.Time    `gorm:"column:EndTimestamp; type:datatime; not null" json:"endTimestamp"`
}

func (HubRequest) TableName() string {
	return "HubRequest"
}

type Object struct {
	ObjectID       cdssdk.ObjectID  `gorm:"column:ObjectID; primaryKey; type:bigint; autoIncrement" json:"objectID"`
	PackageID      cdssdk.PackageID `gorm:"column:PackageID; type:bigint; not null" json:"packageID"`
	Path           string           `gorm:"column:Path; type:varchar(1024); not null" json:"path"`
	Size           int64            `gorm:"column:Size; type:bigint; not null" json:"size"`
	FileHash       string           `gorm:"column:FileHash; type:varchar(255); not null" json:"fileHash"`
	Status         Status           `gorm:"column:Status; type:tinyint; not null" json:"status"`
	FaultTolerance float64          `gorm:"column:faultTolerance; type:float; not null" json:"faultTolerance"`
	Redundancy     float64          `gorm:"column:redundancy; type:float; not null" json:"redundancy"`
	AvgAccessCost  float64          `gorm:"column:avgAccessCost; type:float; not null" json:"avgAccessCost"`
	Timestamp      time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}

func (Object) TableName() string {
	return "Object"
}

type BlockDistribution struct {
	BlockID   int64            `gorm:"column:BlockID; primaryKey; type:bigint; autoIncrement" json:"blockID"`
	ObjectID  cdssdk.ObjectID  `gorm:"column:ObjectID; type:bigint; not null" json:"objectID"`
	Type      string           `gorm:"column:Type; type:varchar(1024); not null" json:"type"`
	Index     int64            `gorm:"column:Index; type:bigint; not null" json:"index"`
	StorageID cdssdk.StorageID `gorm:"column:StorageID; type:bigint; not null" json:"storageID"`
	Status    Status           `gorm:"column:Status; type:tinyint; not null" json:"status"`
	Timestamp time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}

func (BlockDistribution) TableName() string {
	return "BlockDistribution"
}

type StorageTransferCount struct {
	RelationshipID    RelationshipID   `gorm:"column:RelationshipID; primaryKey; type:bigint; autoIncrement" json:"relationshipID"`
	Status            Status           `gorm:"column:Status; type:tinyint; not null" json:"status"`
	SourceStorageID   cdssdk.StorageID `gorm:"column:SourceStorageID; type:bigint; not null" json:"sourceStorageID"`
	TargetStorageID   cdssdk.StorageID `gorm:"column:TargetStorageID; type:bigint; not null" json:"targetStorageID"`
	DataTransferCount int64            `gorm:"column:DataTransferCount; type:bigint; not null" json:"dataTransferCount"`
	Timestamp         time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}

func (StorageTransferCount) TableName() string {
	return "StorageTransferCount"
}
