package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCheckState struct {
	EventBase
	HubID cdssdk.HubID `json:"hubID"`
}

func NewAgentCheckState(hubID cdssdk.HubID) *AgentCheckState {
	return &AgentCheckState{
		HubID: hubID,
	}
}

func init() {
	Register[*AgentCheckState]()
}
