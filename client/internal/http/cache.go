package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
)

type CacheService struct {
	*Server
}

func (s *Server) Cache() *CacheService {
	return &CacheService{
		Server: s,
	}
}

type CacheMovePackageReq struct {
	UserID    cdssdk.UserID    `json:"userID" binding:"required"`
	PackageID cdssdk.PackageID `json:"packageID" binding:"required"`
	StorageID cdssdk.StorageID `json:"storageID" binding:"required"`
}
type CacheMovePackageResp = cdsapi.CacheMovePackageResp

func (s *CacheService) MovePackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Cache.LoadPackage")

	var req CacheMovePackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	hubID, taskID, err := s.svc.CacheSvc().StartCacheMovePackage(req.UserID, req.PackageID, req.StorageID)
	if err != nil {
		log.Warnf("start cache move package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
		return
	}

	for {
		complete, err := s.svc.CacheSvc().WaitCacheMovePackage(hubID, taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("moving complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(CacheMovePackageResp{}))
			return
		}

		if err != nil {
			log.Warnf("wait moving: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
			return
		}
	}
}
