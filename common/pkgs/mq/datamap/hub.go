package datamap

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type HubService interface {
	GetHubStat(msg *GetHubStat) (*GetHubStatResp, *mq.CodeMessage)
	GetHubTrans(msg *GetHubTrans) (*GetHubTransResp, *mq.CodeMessage)
}

// 查询中心自身当前的总数据量
var _ = Register(Service.GetHubStat)

type GetHubStat struct {
	mq.MessageBodyBase
	HubID cdssdk.HubID `json:"hubID"`
}
type GetHubStatResp struct {
	mq.MessageBodyBase
	HubStat stgmod.HubStat `json:"hubStat"`
}

func NewGetHubStat(hubID cdssdk.HubID) *GetHubStat {
	return &GetHubStat{
		HubID: hubID,
	}
}
func NewGetHubStatResp(hubStat stgmod.HubStat) *GetHubStatResp {
	return &GetHubStatResp{
		HubStat: hubStat,
	}
}
func (client *Client) GetHubStat(msg *GetHubStat) (*GetHubStatResp, error) {
	return mq.Request(Service.GetHubStat, client.rabbitCli, msg)
}

// 查询中心节点传输的总数据量

var _ = Register(Service.GetHubTrans)

type GetHubTrans struct {
	mq.MessageBodyBase
	HubID cdssdk.HubID `json:"hubID"`
}

type GetHubTransResp struct {
	mq.MessageBodyBase
	HubTrans stgmod.HubTrans `json:"hubTrans"`
}

func (client *Client) GetHubTrans(msg *GetHubTrans) (*GetHubTransResp, error) {
	//获取到传输统计之后
	return mq.Request(Service.GetHubTrans, client.rabbitCli, msg)
}
