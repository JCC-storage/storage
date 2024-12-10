package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
)

type BucketService struct {
	*Server
}

func (s *Server) Bucket() *BucketService {
	return &BucketService{
		Server: s,
	}
}

func (s *BucketService) GetByName(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.GetByName")

	var req cdsapi.BucketGetByName
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	bucket, err := s.svc.BucketSvc().GetBucketByName(req.UserID, req.Name)
	if err != nil {
		log.Warnf("getting bucket by name: %s", err.Error())
		ctx.JSON(http.StatusOK, FailedError(err))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.BucketGetByNameResp{
		Bucket: bucket,
	}))
}

func (s *BucketService) Create(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.Create")

	var req cdsapi.BucketCreate
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	bucket, err := s.svc.BucketSvc().CreateBucket(req.UserID, req.Name)
	if err != nil {
		log.Warnf("creating bucket: %s", err.Error())
		ctx.JSON(http.StatusOK, FailedError(err))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.BucketCreateResp{
		Bucket: bucket,
	}))
}

func (s *BucketService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.Delete")

	var req cdsapi.BucketDelete
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	if err := s.svc.BucketSvc().DeleteBucket(req.UserID, req.BucketID); err != nil {
		log.Warnf("deleting bucket: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "delete bucket failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}

func (s *BucketService) ListUserBuckets(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.ListUserBuckets")

	var req cdsapi.BucketListUserBucketsReq
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

	ctx.JSON(http.StatusOK, OK(cdsapi.BucketListUserBucketsResp{
		Buckets: buckets,
	}))
}
