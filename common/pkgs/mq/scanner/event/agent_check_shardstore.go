package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCheckShardStore struct {
	EventBase
	StorageID cdssdk.StorageID `json:"storageID"`
}

func NewAgentCheckShardStore(stgID cdssdk.StorageID) *AgentCheckShardStore {
	return &AgentCheckShardStore{
		StorageID: stgID,
	}
}

func init() {
	Register[*AgentCheckShardStore]()
}
