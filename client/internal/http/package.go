package http

import (
	"mime/multipart"
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"

	stgiter "gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

// PackageService 包服务，负责处理包相关的HTTP请求。
type PackageService struct {
	*Server
}

// Package 返回PackageService的实例。
func (s *Server) Package() *PackageService {
	return &PackageService{
		Server: s,
	}
}

func (s *PackageService) Get(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.Get")

	var req cdsapi.PackageGetReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	pkg, err := s.svc.PackageSvc().Get(req.UserID, req.PackageID)
	if err != nil {
		log.Warnf("getting package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageGetResp{Package: *pkg}))
}

func (s *PackageService) GetByName(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetByName")

	var req cdsapi.PackageGetByName
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	pkg, err := s.svc.PackageSvc().GetByName(req.UserID, req.BucketName, req.PackageName)
	if err != nil {
		log.Warnf("getting package by name: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package by name failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageGetByNameResp{Package: *pkg}))
}

// Create 处理创建新包的HTTP请求。
func (s *PackageService) Create(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.Create")
	var req cdsapi.PackageCreate
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	pkg, err := s.svc.PackageSvc().Create(req.UserID, req.BucketID, req.Name)
	if err != nil {
		log.Warnf("creating package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "create package failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageCreateResp{
		Package: pkg,
	}))
}

func (s *PackageService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.Delete")

	var req cdsapi.PackageDelete
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	err := s.svc.PackageSvc().DeletePackage(req.UserID, req.PackageID)
	if err != nil {
		log.Warnf("deleting package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "delete package failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}

func (s *PackageService) ListBucketPackages(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.ListBucketPackages")

	var req cdsapi.PackageListBucketPackages
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	pkgs, err := s.svc.PackageSvc().GetBucketPackages(req.UserID, req.BucketID)
	if err != nil {
		log.Warnf("getting bucket packages: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get bucket packages failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageListBucketPackagesResp{
		Packages: pkgs,
	}))
}

// GetCachedNodes 处理获取包的缓存节点的HTTP请求。
func (s *PackageService) GetCachedNodes(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetCachedNodes")

	var req cdsapi.PackageGetCachedNodesReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	resp, err := s.svc.PackageSvc().GetCachedNodes(req.UserID, req.PackageID)
	if err != nil {
		log.Warnf("get package cached nodes failed: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package cached nodes failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageGetCachedNodesResp{PackageCachingInfo: resp}))
}

// GetLoadedNodes 处理获取包的加载节点的HTTP请求。
func (s *PackageService) GetLoadedNodes(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetLoadedNodes")

	var req cdsapi.PackageGetLoadedNodesReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeIDs, err := s.svc.PackageSvc().GetLoadedNodes(req.UserID, req.PackageID)
	if err != nil {
		log.Warnf("get package loaded nodes failed: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package loaded nodes failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageGetLoadedNodesResp{
		NodeIDs: nodeIDs,
	}))
}

// mapMultiPartFileToUploadingObject 将multipart文件转换为上传对象的迭代器。
func mapMultiPartFileToUploadingObject(files []*multipart.FileHeader) stgiter.UploadingObjectIterator {
	return iterator.Map[*multipart.FileHeader](
		iterator.Array(files...),
		func(file *multipart.FileHeader) (*stgiter.IterUploadingObject, error) {
			stream, err := file.Open()
			if err != nil {
				return nil, err
			}

			fileName, err := url.PathUnescape(file.Filename)
			if err != nil {
				return nil, err
			}

			return &stgiter.IterUploadingObject{
				Path: fileName,
				Size: file.Size,
				File: stream,
			}, nil
		},
	)
}
