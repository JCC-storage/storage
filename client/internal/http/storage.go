package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
)

type StorageService struct {
	*Server
}

func (s *Server) Storage() *StorageService {
	return &StorageService{
		Server: s,
	}
}

func (s *StorageService) LoadPackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.LoadPackage")

	var req cdsapi.StorageLoadPackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	err := s.svc.StorageSvc().LoadPackage(req.UserID, req.PackageID, req.StorageID, req.RootPath)
	if err != nil {
		log.Warnf("loading package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "loading package failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.StorageLoadPackageResp{}))
}

func (s *StorageService) CreatePackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.CreatePackage")

	var req cdsapi.StorageCreatePackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	hubID, taskID, err := s.svc.StorageSvc().StartStorageCreatePackage(
		req.UserID, req.BucketID, req.Name, req.StorageID, req.Path, req.StorageAffinity)
	if err != nil {
		log.Warnf("start storage create package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage create package failed"))
		return
	}

	for {
		complete, packageID, err := s.svc.StorageSvc().WaitStorageCreatePackage(hubID, taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("creating complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage create package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(cdsapi.StorageCreatePackageResp{
				PackageID: packageID,
			}))
			return
		}

		if err != nil {
			log.Warnf("wait creating: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage create package failed"))
			return
		}
	}
}

func (s *StorageService) Get(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.Get")

	var req cdsapi.StorageGet
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	info, err := s.svc.StorageSvc().Get(req.UserID, req.StorageID)
	if err != nil {
		log.Warnf("getting info: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get storage inf failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.StorageGetResp{
		Storage: *info,
	}))
}
