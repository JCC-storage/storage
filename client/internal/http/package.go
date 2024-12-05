package http

import (
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
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

type PackageCreateLoad struct {
	Info  cdsapi.PackageCreateLoadInfo `form:"info" binding:"required"`
	Files []*multipart.FileHeader      `form:"files"`
}

func (s *PackageService) CreateLoad(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.CreateLoad")

	var req PackageCreateLoad
	if err := ctx.ShouldBind(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	up, err := s.svc.Uploader.BeginCreateLoad(req.Info.UserID, req.Info.BucketID, req.Info.Name, req.Info.LoadTo)
	if err != nil {
		log.Warnf("begin package create load: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("begin package create load: %v", err)))
		return
	}
	defer up.Abort()

	var pathes []string
	for _, file := range req.Files {
		f, err := file.Open()
		if err != nil {
			log.Warnf("open file: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("open file %v: %v", file.Filename, err)))
			return
		}

		path, err := url.PathUnescape(file.Filename)
		if err != nil {
			log.Warnf("unescape filename: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("unescape filename %v: %v", file.Filename, err)))
			return
		}
		path = filepath.ToSlash(path)

		err = up.Upload(path, file.Size, f)
		if err != nil {
			log.Warnf("uploading file: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("uploading file %v: %v", file.Filename, err)))
			return
		}
		pathes = append(pathes, path)
	}

	ret, err := up.Commit()
	if err != nil {
		log.Warnf("commit create load: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("commit create load: %v", err)))
		return
	}

	objs := make([]cdssdk.Object, len(pathes))
	for i := range pathes {
		objs[i] = ret.Objects[pathes[i]]
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageCreateLoadResp{Package: ret.Package, Objects: objs, LoadedDirs: ret.LoadedDirs}))

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

// GetCachedStorages 处理获取包的缓存节点的HTTP请求。
func (s *PackageService) GetCachedStorages(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetCachedStorages")

	var req cdsapi.PackageGetCachedStoragesReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	resp, err := s.svc.PackageSvc().GetCachedStorages(req.UserID, req.PackageID)
	if err != nil {
		log.Warnf("get package cached storages failed: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package cached storages failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageGetCachedStoragesResp{PackageCachingInfo: resp}))
}

// GetLoadedStorages 处理获取包的加载节点的HTTP请求。
func (s *PackageService) GetLoadedStorages(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetLoadedStorages")

	var req cdsapi.PackageGetLoadedStoragesReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	stgIDs, err := s.svc.PackageSvc().GetLoadedStorages(req.UserID, req.PackageID)
	if err != nil {
		log.Warnf("get package loaded storages failed: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package loaded storages failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PackageGetLoadedStoragesResp{
		StorageIDs: stgIDs,
	}))
}
