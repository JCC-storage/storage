package datamap

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type ObjectService interface {
	GetBlockTransInfo(msg *GetBlockTransInfo) (*GetBlockTransInfoResp, *mq.CodeMessage)
	GetBlockDistribution(msg *GetBlockDistribution) (*GetBlockDistributionResp, *mq.CodeMessage)
}

type GetBlockTransInfo struct {
	mq.MessageBodyBase
	//todo 入参待确认
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}

type GetBlockTransInfoResp struct {
	mq.MessageBodyBase
	ObjectTransInfo stgmod.BlockTransInfo `json:"objectTransInfo"`
}

type GetBlockDistribution struct {
	mq.MessageBodyBase
	//todo 入参待确认
	HubID cdssdk.HubID `json:"hubID"`
}

type GetBlockDistributionResp struct {
	mq.MessageBodyBase
	BlockDistribution stgmod.BlockDistribution `json:"blockDistribution"`
}
