package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	stgsdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type StorageService struct {
	*Server
}

func (s *Server) StorageSvc() *StorageService {
	return &StorageService{
		Server: s,
	}
}

type StorageLoadPackageReq struct {
	UserID    *int64 `json:"userID" binding:"required"`
	PackageID *int64 `json:"packageID" binding:"required"`
	StorageID *int64 `json:"storageID" binding:"required"`
}

type StorageLoadPackageResp struct {
	stgsdk.StorageLoadPackageResp
}

func (s *StorageService) LoadPackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.LoadPackage")

	var req StorageLoadPackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	taskID, err := s.svc.StorageSvc().StartStorageLoadPackage(*req.UserID, *req.PackageID, *req.StorageID)
	if err != nil {
		log.Warnf("start storage load package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage load package failed"))
		return
	}

	for {
		complete, fullPath, err := s.svc.StorageSvc().WaitStorageLoadPackage(taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("loading complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage load package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(StorageLoadPackageResp{
				StorageLoadPackageResp: stgsdk.StorageLoadPackageResp{
					FullPath: fullPath,
				},
			}))
			return
		}

		if err != nil {
			log.Warnf("wait loadding: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage load package failed"))
			return
		}
	}
}

type StorageCreatePackageReq struct {
	UserID       *int64                     `json:"userID" binding:"required"`
	StorageID    *int64                     `json:"storageID" binding:"required"`
	Path         string                     `json:"path" binding:"required"`
	BucketID     *int64                     `json:"bucketID" binding:"required"`
	Name         string                     `json:"name" binding:"required"`
	Redundancy   stgsdk.TypedRedundancyInfo `json:"redundancy" binding:"required"`
	NodeAffinity *int64                     `json:"nodeAffinity"`
}

type StorageCreatePackageResp struct {
	PackageID int64 `json:"packageID"`
}

func (s *StorageService) CreatePackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.CreatePackage")

	var req StorageCreatePackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeID, taskID, err := s.svc.StorageSvc().StartStorageCreatePackage(
		*req.UserID, *req.BucketID, req.Name, *req.StorageID, req.Path, req.Redundancy, req.NodeAffinity)
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

			ctx.JSON(http.StatusOK, OK(StorageCreatePackageResp{
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

type StorageGetInfoReq struct {
	UserID    *int64 `form:"userID" binding:"required"`
	StorageID *int64 `form:"storageID" binding:"required"`
}

type StorageGetInfoResp struct {
	stgsdk.StorageGetInfoResp
}

func (s *StorageService) GetInfo(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.GetInfo")

	var req StorageGetInfoReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	info, err := s.svc.StorageSvc().GetInfo(*req.UserID, *req.StorageID)
	if err != nil {
		log.Warnf("getting info: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get storage inf failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(StorageGetInfoResp{
		StorageGetInfoResp: stgsdk.StorageGetInfoResp{
			Name:      info.Name,
			NodeID:    info.NodeID,
			Directory: info.Directory,
		},
	}))
}
