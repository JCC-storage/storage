package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
)

type HubService struct {
	*Server
}

func (s *Server) HubSvc() *HubService {
	return &HubService{
		Server: s,
	}
}

type GetHubsReq struct {
	HubIDs *[]cdssdk.HubID `form:"hubIDs" binding:"required"`
}
type GetHubsResp = cdsapi.HubGetHubsResp

func (s *ObjectService) GetHubs(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Hub.GetHubs")

	var req GetHubsReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	hubs, err := s.svc.HubSvc().GetHubs(*req.HubIDs)
	if err != nil {
		log.Warnf("getting hubs: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get hubs failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(GetHubsResp{Hubs: hubs}))
}
