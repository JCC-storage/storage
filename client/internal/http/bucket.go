package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// BucketService 用于处理与存储桶相关的HTTP请求
type BucketService struct {
	*Server
}

// Bucket 返回BucketService的实例
func (s *Server) Bucket() *BucketService {
	return &BucketService{
		Server: s,
	}
}

// Create 创建一个新的存储桶
// ctx *gin.Context: Gin框架的上下文对象，用于处理HTTP请求和响应
func (s *BucketService) Create(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.Create")

	var req cdssdk.BucketCreateReq
	// 尝试从HTTP请求绑定JSON请求体到结构体
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		// 绑定失败，返回错误信息
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	// 调用服务层方法，创建存储桶
	bucketID, err := s.svc.BucketSvc().CreateBucket(req.UserID, req.BucketName)
	if err != nil {
		log.Warnf("creating bucket: %s", err.Error())
		// 创建存储桶失败，返回错误信息
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "create bucket failed"))
		return
	}

	// 创建存储桶成功，返回成功响应
	ctx.JSON(http.StatusOK, OK(cdssdk.BucketCreateResp{
		BucketID: bucketID,
	}))
}

// Delete 删除指定的存储桶
// ctx *gin.Context: Gin框架的上下文对象，用于处理HTTP请求和响应
func (s *BucketService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.Delete")

	var req cdssdk.BucketDeleteReq
	// 尝试从HTTP请求绑定JSON请求体到结构体
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		// 绑定失败，返回错误信息
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	// 调用服务层方法，删除存储桶
	if err := s.svc.BucketSvc().DeleteBucket(req.UserID, req.BucketID); err != nil {
		log.Warnf("deleting bucket: %s", err.Error())
		// 删除存储桶失败，返回错误信息
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "delete bucket failed"))
		return
	}

	// 删除存储桶成功，返回成功响应
	ctx.JSON(http.StatusOK, OK(nil))
}
