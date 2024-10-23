package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
)

type AgentService interface {
	GetState(msg *GetState) (*GetStateResp, *mq.CodeMessage)
}

// 获取agent状态
var _ = Register(Service.GetState)

type GetState struct {
	mq.MessageBodyBase
}
type GetStateResp struct {
	mq.MessageBodyBase
}

func NewGetState() *GetState {
	return &GetState{}
}
func NewGetStateResp() *GetStateResp {
	return &GetStateResp{}
}
func (client *Client) GetState(msg *GetState, opts ...mq.RequestOption) (*GetStateResp, error) {
	return mq.Request(Service.GetState, client.rabbitCli, msg, opts...)
}
