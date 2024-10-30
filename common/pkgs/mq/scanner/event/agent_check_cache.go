package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCheckCache struct {
	EventBase
	StorageID cdssdk.StorageID `json:"storageID"`
}

func NewAgentCheckCache(stgID cdssdk.StorageID) *AgentCheckCache {
	return &AgentCheckCache{
		StorageID: stgID,
	}
}

func init() {
	Register[*AgentCheckCache]()
}
