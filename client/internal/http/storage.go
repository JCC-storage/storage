package http

import (
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
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

	var req cdssdk.StorageLoadPackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeID, taskID, err := s.svc.StorageSvc().StartStorageLoadPackage(req.UserID, req.PackageID, req.StorageID)
	if err != nil {
		log.Warnf("start storage load package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("start loading: %v", err)))
		return
	}

	for {
		complete, ret, err := s.svc.StorageSvc().WaitStorageLoadPackage(nodeID, taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("loading complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("loading complete with: %v", err)))
				return
			}

			ctx.JSON(http.StatusOK, OK(cdssdk.StorageLoadPackageResp{
				FullPath:    filepath.Join(ret.RemoteBase, ret.PackagePath),
				PackagePath: ret.PackagePath,
				LocalBase:   ret.LocalBase,
				RemoteBase:  ret.RemoteBase,
			}))
			return
		}

		if err != nil {
			log.Warnf("wait loadding: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("wait loading: %v", err)))
			return
		}
	}
}

func (s *StorageService) CreatePackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.CreatePackage")

	var req cdssdk.StorageCreatePackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeID, taskID, err := s.svc.StorageSvc().StartStorageCreatePackage(
		req.UserID, req.BucketID, req.Name, req.StorageID, req.Path, req.NodeAffinity)
	if err != nil {
		log.Warnf("start storage create package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage create package failed"))
		return
	}

	for {
		complete, packageID, err := s.svc.StorageSvc().WaitStorageCreatePackage(nodeID, taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("creating complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage create package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(cdssdk.StorageCreatePackageResp{
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

	var req cdssdk.StorageGet
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

	ctx.JSON(http.StatusOK, OK(cdssdk.StorageGetResp{
		Storage: *info,
	}))
}
