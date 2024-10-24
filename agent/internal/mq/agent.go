package mq

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) GetState(msg *agtmq.GetState) (*agtmq.GetStateResp, *mq.CodeMessage) {
	return mq.ReplyOK(agtmq.NewGetStateResp())
}
