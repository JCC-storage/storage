package models

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gorm.io/gorm"
	"time"
)

var DB *gorm.DB

// InitDB 初始化数据库连接
func InitDB(db *gorm.DB) {
	DB = db
}

type RequestID int64
type BlockID int64
type RelationshipID int64
type Status int

const (
	StatusNow                  = 0 //表示当前实时状态
	StatusYesterdayAfter       = 1 // 表示前一天调整后的状态
	StatusYesterdayBefore      = 2 // 表示前一天调整前的状态
	StatusTodayBeforeYesterday = 3 // 表示前两天调整后的状态
)

// 节点间关系图

type HubRelationship struct {
	Nodes []Node `json:"nodes"` // 节点列表
	Edges []Edge `json:"edges"` // 名称
}

type Node struct {
	ID           string                `json:"id"`           // 节点/中心ID
	NodeType     string                `json:"nodeType"`     //节点类型 storage/hub
	Name         string                `json:"name"`         // 节点/中心名称
	Address      cdssdk.HubAddressInfo `json:"address"`      // 地址
	DataCount    int64                 `json:"dataCount"`    // 总数据量（文件分块数）
	NewDataCount int64                 `json:"newdataCount"` // 新增数据量（文件分块数）
	Timestamp    time.Time             `json:"timestamp"`    // 时间戳
}

type Edge struct {
	SourceType         string    `json:"sourceType"`         // 源节点类型
	SourceID           string    `json:"source"`             // 源节点ID
	TargetType         string    `json:"targetType"`         // 目标节点类型
	TargetID           string    `json:"target"`             // 目标节点ID
	DataTransferCount  int64     `json:"dataTransferCount"`  // 数据传输量
	RequestCount       int64     `json:"requestCount"`       // 请求数
	FailedRequestCount int64     `json:"failedRequestCount"` // 失败请求数
	AvgTransferCount   int64     `json:"avgTransferCount"`   // 平均数据传输量
	MaxTransferCount   int64     `json:"maxTransferCount"`   // 最大数据传输量
	MinTransferCount   int64     `json:"minTransferCount"`   // 最小数据传输量
	StartTimestamp     time.Time `json:"startTimestamp"`     // 起始时间戳
	EndTimestamp       time.Time `json:"endTimestamp"`       // 结束时间戳
}

// 对象分布
type ObjectDistribution struct {
	Nodes  []DistNode `json:"nodes"`
	Combos []Combo    `json:"combos"`
	Edges  []DistEdge `json:"edges"`
}

type DistNode struct {
	ID       string `json:"id"`
	ComboID  string `json:"comboId"`
	Label    string `json:"label"`
	NodeType string `json:"nodeType"`
}

type Combo struct {
	ID        string `json:"id"`
	Label     string `json:"label"`
	ParentId  string `json:"parentId"`
	ComboType string `json:"comboType"`
}

type DistEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

// 对象块分布结构
//type ObjectDistribution struct {
//	ObjectID      cdssdk.ObjectID  `json:"objectID"`      // 对象 ID
//	PackageID     cdssdk.PackageID `json:"packageID"`     // 包 ID
//	Path          string           `json:"path"`          // 路径
//	Size          int64            `json:"size"`          // 大小
//	FileHash      string           `json:"fileHash"`      // 文件哈希
//	States        []State          `json:"states"`        // 各阶段状态信息（只需要传1、2、3阶段）
//	Relationships []Relationship   `json:"relationships"` // 节点间传输量
//	Timestamp     time.Time        `json:"timestamp"`     // 请求中的时间戳
//}
//
//type State struct {
//	Timestamp          time.Time   `json:"timestamp"`          // 时间戳
//	Status             string      `json:"status"`             // 状态
//	FaultTolerance     string      `json:"faultTolerance"`     // 容灾度（仅布局调整后）
//	Redundancy         string      `json:"redundancy"`         // 冗余度（仅布局调整后）
//	AvgAccessCost      float64     `json:"avgAccessCost"`      // 平均访问开销（仅布局调整前）
//	BlockDistributions []BlockDist `json:"blockDistributions"` // 块分布情况
//}
//
//type BlockDist struct {
//	StorageID cdssdk.StorageID `json:"storageID"` // 中心ID
//	Blocks    []Block          `json:"blocks"`    // 该中心的所有块
//}
//
//type Block struct {
//	Type  string `json:"type"`  // 块类型
//	Index string `json:"index"` // 块编号
//	ID    string `json:"id"`    // 块ID
//}
//
//type Relationship struct {
//	Status            Status    `json:"status"`            // 连线左侧的状态
//	SourceStorageID   string    `json:"sourceStorageID"`   // 源存储节点 ID
//	TargetStorageID   string    `json:"targetStorageID"`   // 目标存储节点 ID
//	DataTransferCount string    `json:"dataTransferCount"` // 数据传输量
//	Timestamp         time.Time `json:"timestamp"`         // 变化结束时间戳
//}
