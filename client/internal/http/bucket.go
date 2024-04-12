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

func (s *BucketService) GetByName(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.GetByName")

	var req cdssdk.BucketGetByName
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	bucket, err := s.svc.BucketSvc().GetBucketByName(req.UserID, req.Name)
	if err != nil {
		log.Warnf("getting bucket by name: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get bucket by name failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdssdk.BucketGetByNameResp{
		Bucket: bucket,
	}))
}

func (s *BucketService) Create(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.Create")

	var req cdssdk.BucketCreate
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		// 绑定失败，返回错误信息
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	bucket, err := s.svc.BucketSvc().CreateBucket(req.UserID, req.Name)
	if err != nil {
		log.Warnf("creating bucket: %s", err.Error())
		// 创建存储桶失败，返回错误信息
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "create bucket failed"))
		return
	}

	// 创建存储桶成功，返回成功响应
	ctx.JSON(http.StatusOK, OK(cdssdk.BucketCreateResp{
		Bucket: bucket,
	}))
}

// Delete 删除指定的存储桶
// ctx *gin.Context: Gin框架的上下文对象，用于处理HTTP请求和响应
func (s *BucketService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.Delete")

	var req cdssdk.BucketDelete
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

func (s *BucketService) ListUserBuckets(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.ListUserBuckets")

	var req cdssdk.BucketListUserBucketsReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	buckets, err := s.svc.BucketSvc().GetUserBuckets(req.UserID)
	if err != nil {
		log.Warnf("getting user buckets: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get user buckets failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdssdk.BucketListUserBucketsResp{
		Buckets: buckets,
	}))
}
