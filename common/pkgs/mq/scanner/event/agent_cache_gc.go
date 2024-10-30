package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCacheGC struct {
	EventBase
	StorageID cdssdk.StorageID `json:"storageID"`
}

func NewAgentCacheGC(stgID cdssdk.StorageID) *AgentCacheGC {
	return &AgentCacheGC{
		StorageID: stgID,
	}
}

func init() {
	Register[*AgentCacheGC]()
}
