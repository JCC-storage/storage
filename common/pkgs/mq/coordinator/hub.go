package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type HubService interface {
	GetHubConfig(msg *GetHubConfig) (*GetHubConfigResp, *mq.CodeMessage)

	GetUserHubs(msg *GetUserHubs) (*GetUserHubsResp, *mq.CodeMessage)

	GetHubs(msg *GetHubs) (*GetHubsResp, *mq.CodeMessage)

	GetHubConnectivities(msg *GetHubConnectivities) (*GetHubConnectivitiesResp, *mq.CodeMessage)

	UpdateHubConnectivities(msg *UpdateHubConnectivities) (*UpdateHubConnectivitiesResp, *mq.CodeMessage)
}

var _ = Register(Service.GetHubConfig)

type GetHubConfig struct {
	mq.MessageBodyBase
	HubID cdssdk.HubID `json:"hubID"`
}
type GetHubConfigResp struct {
	mq.MessageBodyBase
	Hub      cdssdk.Hub             `json:"hub"`
	Storages []stgmod.StorageDetail `json:"storages"`
}

func ReqGetHubConfig(hubID cdssdk.HubID) *GetHubConfig {
	return &GetHubConfig{
		HubID: hubID,
	}
}
func RespGetHubConfig(hub cdssdk.Hub, storages []stgmod.StorageDetail) *GetHubConfigResp {
	return &GetHubConfigResp{
		Hub:      hub,
		Storages: storages,
	}
}
func (client *Client) GetHubConfig(msg *GetHubConfig) (*GetHubConfigResp, error) {
	return mq.Request(Service.GetHubConfig, client.rabbitCli, msg)
}

// 查询用户可用的节点
var _ = Register(Service.GetUserHubs)

type GetUserHubs struct {
	mq.MessageBodyBase
	UserID cdssdk.UserID `json:"userID"`
}
type GetUserHubsResp struct {
	mq.MessageBodyBase
	Hubs []cdssdk.Hub `json:"hubs"`
}

func NewGetUserHubs(userID cdssdk.UserID) *GetUserHubs {
	return &GetUserHubs{
		UserID: userID,
	}
}
func NewGetUserHubsResp(hubs []cdssdk.Hub) *GetUserHubsResp {
	return &GetUserHubsResp{
		Hubs: hubs,
	}
}
func (client *Client) GetUserHubs(msg *GetUserHubs) (*GetUserHubsResp, error) {
	return mq.Request(Service.GetUserHubs, client.rabbitCli, msg)
}

// 获取指定节点的信息。如果HubIDs为nil，则返回所有Hub
var _ = Register(Service.GetHubs)

type GetHubs struct {
	mq.MessageBodyBase
	HubIDs []cdssdk.HubID `json:"hubIDs"`
}
type GetHubsResp struct {
	mq.MessageBodyBase
	Hubs []*cdssdk.Hub `json:"hubs"`
}

func NewGetHubs(hubIDs []cdssdk.HubID) *GetHubs {
	return &GetHubs{
		HubIDs: hubIDs,
	}
}
func NewGetHubsResp(hubs []*cdssdk.Hub) *GetHubsResp {
	return &GetHubsResp{
		Hubs: hubs,
	}
}
func (r *GetHubsResp) GetHub(id cdssdk.HubID) *cdssdk.Hub {
	for _, n := range r.Hubs {
		if n.HubID == id {
			return n
		}
	}

	return nil
}
func (client *Client) GetHubs(msg *GetHubs) (*GetHubsResp, error) {
	return mq.Request(Service.GetHubs, client.rabbitCli, msg)
}

// 获取节点连通性信息
var _ = Register(Service.GetHubConnectivities)

type GetHubConnectivities struct {
	mq.MessageBodyBase
	HubIDs []cdssdk.HubID `json:"hubIDs"`
}
type GetHubConnectivitiesResp struct {
	mq.MessageBodyBase
	Connectivities []cdssdk.HubConnectivity `json:"hubs"`
}

func ReqGetHubConnectivities(hubIDs []cdssdk.HubID) *GetHubConnectivities {
	return &GetHubConnectivities{
		HubIDs: hubIDs,
	}
}
func RespGetHubConnectivities(cons []cdssdk.HubConnectivity) *GetHubConnectivitiesResp {
	return &GetHubConnectivitiesResp{
		Connectivities: cons,
	}
}
func (client *Client) GetHubConnectivities(msg *GetHubConnectivities) (*GetHubConnectivitiesResp, error) {
	return mq.Request(Service.GetHubConnectivities, client.rabbitCli, msg)
}

// 批量更新节点连通性信息
var _ = Register(Service.UpdateHubConnectivities)

type UpdateHubConnectivities struct {
	mq.MessageBodyBase
	Connectivities []cdssdk.HubConnectivity `json:"connectivities"`
}
type UpdateHubConnectivitiesResp struct {
	mq.MessageBodyBase
}

func ReqUpdateHubConnectivities(cons []cdssdk.HubConnectivity) *UpdateHubConnectivities {
	return &UpdateHubConnectivities{
		Connectivities: cons,
	}
}
func RespUpdateHubConnectivities() *UpdateHubConnectivitiesResp {
	return &UpdateHubConnectivitiesResp{}
}
func (client *Client) UpdateHubConnectivities(msg *UpdateHubConnectivities) (*UpdateHubConnectivitiesResp, error) {
	return mq.Request(Service.UpdateHubConnectivities, client.rabbitCli, msg)
}
