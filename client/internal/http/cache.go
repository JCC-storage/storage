package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// CacheService 缓存服务结构体，依赖于Server
type CacheService struct {
	*Server
}

// Cache 返回CacheService的实例
func (s *Server) Cache() *CacheService {
	return &CacheService{
		Server: s,
	}
}

// CacheMovePackageReq 移动缓存包的请求参数
type CacheMovePackageReq struct {
	UserID    *cdssdk.UserID    `json:"userID" binding:"required"`
	PackageID *cdssdk.PackageID `json:"packageID" binding:"required"`
	NodeID    *cdssdk.NodeID    `json:"nodeID" binding:"required"`
}

// CacheMovePackageResp 移动缓存包的响应参数
type CacheMovePackageResp = cdssdk.CacheMovePackageResp

// MovePackage 处理移动缓存包的请求
func (s *CacheService) MovePackage(ctx *gin.Context) {
	// 初始化日志
	log := logger.WithField("HTTP", "Cache.LoadPackage")

	// 绑定请求JSON
	var req CacheMovePackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	// 开始移动缓存包任务
	taskID, err := s.svc.CacheSvc().StartCacheMovePackage(*req.UserID, *req.PackageID, *req.NodeID)
	if err != nil {
		log.Warnf("start cache move package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
		return
	}

	// 循环等待缓存包移动完成
	for {
		// 检查移动是否完成
		complete, err := s.svc.CacheSvc().WaitCacheMovePackage(*req.NodeID, taskID, time.Second*10)
		if complete {
			// 移动完成后的处理
			if err != nil {
				log.Warnf("moving complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(CacheMovePackageResp{}))
			return
		}

		// 等待移动过程中的错误处理
		if err != nil {
			log.Warnf("wait moving: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
			return
		}
	}
}
