package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCheckStorage struct {
	EventBase
	StorageID cdssdk.StorageID `json:"storageID"`
}

func NewAgentCheckStorage(storageID cdssdk.StorageID) *AgentCheckStorage {
	return &AgentCheckStorage{
		StorageID: storageID,
	}
}

func init() {
	Register[*AgentCheckStorage]()
}
