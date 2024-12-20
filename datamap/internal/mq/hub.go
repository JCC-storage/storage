package mq

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	datamapmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/datamap"
)

func (svc *Service) GetHubStat(msg *datamapmq.GetHubStat) (*datamapmq.GetHubStatResp, *mq.CodeMessage) {
	logger.WithField("HubID", msg.HubID)

	//从datamapmq队列接收数据到stgmod.HubStat

	//从datamapmq队列接收数据到stgmod.HubStat,然后将该数据和mysql数据库中获取到的models.Storage做对比，用stgmode
	return &datamapmq.GetHubStatResp{}, nil
}

func (svc *Service) GetHubTrans(msg *datamapmq.GetHubTrans) (*datamapmq.GetHubTransResp, *mq.CodeMessage) {
	logger.WithField("HubID", msg.HubID)

	return &datamapmq.GetHubTransResp{}, nil
}
