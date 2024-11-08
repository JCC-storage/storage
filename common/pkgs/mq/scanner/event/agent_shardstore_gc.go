package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentShardStoreGC struct {
	EventBase
	StorageID cdssdk.StorageID `json:"storageID"`
}

func NewAgentShardStoreGC(stgID cdssdk.StorageID) *AgentShardStoreGC {
	return &AgentShardStoreGC{
		StorageID: stgID,
	}
}

func init() {
	Register[*AgentShardStoreGC]()
}
