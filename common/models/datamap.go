package stgmod

import "time"

// HubStat 中心信息
// 每天一次，各节点统计自身当前的总数据量
type HubStat struct {
	Timestamp time.Time `json:"timestamp"`
	Source    struct {
		Type    string `json:"type"`
		HubID   string `json:"hubID"`
		HubName string `json:"hubName"`
	} `json:"source"`
	Category string `json:"category"`
	Body     struct {
		MasterHubID      int    `json:"masterHubID"`
		MasterHubAddress string `json:"masterHubAddress"`
		StorageID        int    `json:"storageID"`
		DataCount        int    `json:"dataCount"`
	} `json:"body"`
}

// HubTrans 节点传输信息
// 每天一次，各节点统计自身当天向外部各个节点传输的总数据量
type HubTrans struct {
	Timestamp time.Time `json:"timestamp"`
	Source    struct {
		Type    string `json:"type"`
		HubID   string `json:"hubID"`
		HubName string `json:"hubName"`
	} `json:"source"`
	Category string `json:"category"`
	Body     struct {
		SourceHubID        int       `json:"sourceHubID"`
		TargetHubID        int       `json:"targetHubID"`
		DataTransferCount  int       `json:"dataTransferCount"`
		RequestCount       int       `json:"requestCount"`
		FailedRequestCount int       `json:"failedRequestCount"`
		AvgTransferCount   int       `json:"avgTransferCount"`
		MaxTransferCount   int       `json:"maxTransferCount"`
		MinTransferCount   int       `json:"minTransferCount"`
		StartTimestamp     time.Time `json:"startTimestamp"`
		EndTimestamp       time.Time `json:"endTimestamp"`
	} `json:"body"`
}

// BlockTransInfo 块传输信息
/*实时日志，当对象的块信息发生变化时推送，共有4种变化类型：
拷贝
编解码(一变多、多变一、多变多)
删除
更新*/
type BlockTransInfo struct {
	Timestamp time.Time `json:"timestamp"`
	Source    struct {
		Type    string      `json:"type"`
		HubID   interface{} `json:"hubID"`
		HubName interface{} `json:"hubName"`
	} `json:"source"`
	Category string `json:"category"`
	Body     struct {
		Type         string      `json:"type"`
		ObjectID     interface{} `json:"objectID"`
		PackageID    interface{} `json:"packageID"`
		BlockChanges []struct {
			Type              string      `json:"type"`
			BlockType         interface{} `json:"blockType"`
			Index             interface{} `json:"index"`
			SourceStorageID   interface{} `json:"sourceStorageID"`
			TargetStorageID   interface{} `json:"targetStorageID"`
			DataTransferCount interface{} `json:"dataTransferCount"`
			Timestamp         interface{} `json:"timestamp"`
			SourceBlocks      []struct {
				BlockType interface{} `json:"blockType"`
				Index     interface{} `json:"index"`
				StorageID interface{} `json:"storageID"`
			} `json:"sourceBlocks,omitempty"`
			TargetBlocks []struct {
				BlockType interface{} `json:"blockType"`
				Index     interface{} `json:"index"`
				StorageID interface{} `json:"storageID"`
			} `json:"targetBlocks,omitempty"`
			DataTransfers []struct {
				SourceStorageID   interface{} `json:"sourceStorageID"`
				TargetStorageID   interface{} `json:"targetStorageID"`
				DataTransferCount interface{} `json:"dataTransferCount"`
			} `json:"dataTransfers,omitempty"`
			Blocks []struct {
				BlockType interface{} `json:"blockType"`
				Index     interface{} `json:"index"`
				StorageID interface{} `json:"storageID"`
			} `json:"blocks,omitempty"`
		} `json:"blockChanges"`
	} `json:"body"`
}

// BlockDistribution 块传输信息
// 每天一次，在调整完成后，将当天调整前后的布局情况一起推送
type BlockDistribution struct {
	Timestamp time.Time `json:"timestamp"`
	Source    struct {
		Type    string      `json:"type"`
		HubID   interface{} `json:"hubID"`
		HubName interface{} `json:"hubName"`
	} `json:"source"`
	Category string `json:"category"`
	Body     struct {
		Type         string      `json:"type"`
		ObjectID     interface{} `json:"objectID"`
		PackageID    interface{} `json:"packageID"`
		BlockChanges []struct {
			Type              string      `json:"type"`
			BlockType         interface{} `json:"blockType"`
			Index             interface{} `json:"index"`
			SourceStorageID   interface{} `json:"sourceStorageID"`
			TargetStorageID   interface{} `json:"targetStorageID"`
			DataTransferCount interface{} `json:"dataTransferCount"`
			Timestamp         interface{} `json:"timestamp"`
			SourceBlocks      []struct {
				BlockType interface{} `json:"blockType"`
				Index     interface{} `json:"index"`
				StorageID interface{} `json:"storageID"`
			} `json:"sourceBlocks,omitempty"`
			TargetBlocks []struct {
				BlockType interface{} `json:"blockType"`
				Index     interface{} `json:"index"`
				StorageID interface{} `json:"storageID"`
			} `json:"targetBlocks,omitempty"`
			DataTransfers []struct {
				SourceStorageID   interface{} `json:"sourceStorageID"`
				TargetStorageID   interface{} `json:"targetStorageID"`
				DataTransferCount interface{} `json:"dataTransferCount"`
			} `json:"dataTransfers,omitempty"`
			Blocks []struct {
				BlockType interface{} `json:"blockType"`
				Index     interface{} `json:"index"`
				StorageID interface{} `json:"storageID"`
			} `json:"blocks,omitempty"`
		} `json:"blockChanges"`
	} `json:"body"`
}

// ObjectUpdateInfo Object变化信息
type ObjectUpdateInfo struct {
	Timestamp time.Time `json:"timestamp"`
	Source    struct {
		Type    string `json:"type"`
		HubID   string `json:"hubID"`
		HubName string `json:"hubName"`
	} `json:"source"`
	Category string `json:"category"`
	Body     struct {
		Type              string `json:"type"`
		ObjectID          string `json:"objectID"`
		PackageID         string `json:"packageID"`
		Path              string `json:"path"`
		Size              int    `json:"size"`
		BlockDistribution []struct {
			Type      string `json:"type,omitempty"`
			Index     string `json:"index,omitempty"`
			StorageID string `json:"storageID,omitempty"`
		} `json:"blockDistribution"`
		Timestamp string `json:"timestamp"`
	} `json:"body"`
}

// PackageUpdateInfo package变化信息
type PackageUpdateInfo struct {
	Timestamp time.Time `json:"timestamp"`
	Source    struct {
		Type    string `json:"type"`
		HubID   string `json:"hubID"`
		HubName string `json:"hubName"`
	} `json:"source"`
	Category string `json:"category"`
	Body     struct {
		Type        string `json:"type"`
		PackageID   string `json:"packageID"`
		PackageName string `json:"packageName"`
		BucketID    string `json:"bucketID"`
		Timestamp   string `json:"timestamp"`
	} `json:"body"`
}

// BucketUpdateInfo bucket变化信息
type BucketUpdateInfo struct {
	Timestamp time.Time `json:"timestamp"`
	Source    struct {
		Type    string `json:"type"`
		HubID   string `json:"hubID"`
		HubName string `json:"hubName"`
	} `json:"source"`
	Category string `json:"category"`
	Body     struct {
		Type       string `json:"type"`
		BucketID   string `json:"bucketID"`
		BucketName string `json:"bucketName"`
		Timestamp  string `json:"timestamp"`
	} `json:"body"`
}
