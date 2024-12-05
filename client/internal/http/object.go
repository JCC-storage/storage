package http

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	"gitlink.org.cn/cloudream/storage/client/internal/config"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
)

type ObjectService struct {
	*Server
}

func (s *Server) Object() *ObjectService {
	return &ObjectService{
		Server: s,
	}
}

func (s *ObjectService) List(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.List")

	var req cdsapi.ObjectList
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	objs, err := s.svc.ObjectSvc().List(req.UserID, req.PackageID, req.PathPrefix)
	if err != nil {
		log.Warnf("listing objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("listing objects: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectListResp{Objects: objs}))
}

type ObjectUploadReq struct {
	Info  cdsapi.ObjectUploadInfo `form:"info" binding:"required"`
	Files []*multipart.FileHeader `form:"files"`
}

func (s *ObjectService) Upload(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Upload")

	var req ObjectUploadReq
	if err := ctx.ShouldBind(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	up, err := s.svc.Uploader.BeginUpdate(req.Info.UserID, req.Info.PackageID, req.Info.Affinity)
	if err != nil {
		log.Warnf("begin update: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("begin update: %v", err)))
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
		log.Warnf("commit update: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("commit update: %v", err)))
		return
	}

	uploadeds := make([]cdssdk.Object, len(pathes))
	for i := range pathes {
		uploadeds[i] = ret.Objects[pathes[i]]
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectUploadResp{Uploadeds: uploadeds}))
}

func (s *ObjectService) Download(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Download")

	var req cdsapi.ObjectDownload
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	off := req.Offset
	len := int64(-1)
	if req.Length != nil {
		len = *req.Length
	}

	file, err := s.svc.ObjectSvc().Download(req.UserID, downloader.DownloadReqeust{
		ObjectID: req.ObjectID,
		Offset:   off,
		Length:   len,
	})
	if err != nil {
		log.Warnf("downloading object: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "download object failed"))
		return
	}
	defer file.File.Close()

	ctx.Header("Content-Disposition", "attachment; filename="+url.PathEscape(path.Base(file.Object.Path)))
	ctx.Header("Content-Type", "application/octet-stream")
	ctx.Header("Content-Transfer-Encoding", "binary")

	n, err := io.Copy(ctx.Writer, file.File)
	if err != nil {
		log.Warnf("copying file: %s", err.Error())
	}

	// TODO 当client不在某个代理节点上时如何处理？
	if config.Cfg().StorageID > 0 {
		s.svc.AccessStat.AddAccessCounter(file.Object.ObjectID, file.Object.PackageID, config.Cfg().StorageID, float64(n)/float64(file.Object.Size))
	}
}

func (s *ObjectService) UpdateInfo(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.UpdateInfo")

	var req cdsapi.ObjectUpdateInfo
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	sucs, err := s.svc.ObjectSvc().UpdateInfo(req.UserID, req.Updatings)
	if err != nil {
		log.Warnf("updating objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "update objects failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectUpdateInfoResp{Successes: sucs}))
}

func (s *ObjectService) Move(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Move")

	var req cdsapi.ObjectMove
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	sucs, err := s.svc.ObjectSvc().Move(req.UserID, req.Movings)
	if err != nil {
		log.Warnf("moving objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "move objects failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectMoveResp{Successes: sucs}))
}

func (s *ObjectService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Delete")

	var req cdsapi.ObjectDelete
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	err := s.svc.ObjectSvc().Delete(req.UserID, req.ObjectIDs)
	if err != nil {
		log.Warnf("deleting objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "delete objects failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}

func (s *ObjectService) GetPackageObjects(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.GetPackageObjects")

	var req cdsapi.ObjectGetPackageObjects
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	objs, err := s.svc.ObjectSvc().GetPackageObjects(req.UserID, req.PackageID)
	if err != nil {
		log.Warnf("getting package objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package object failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectGetPackageObjectsResp{Objects: objs}))
}
