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
	"gitlink.org.cn/cloudream/common/utils/math2"
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

func (s *ObjectService) ListByPath(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.ListByPath")

	var req cdsapi.ObjectListByPath
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	objs, err := s.svc.ObjectSvc().GetByPath(req.UserID, req.PackageID, req.Path, req.IsPrefix)
	if err != nil {
		log.Warnf("listing objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("listing objects: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectListByPathResp{Objects: objs}))
}

func (s *ObjectService) ListByIDs(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.ListByIDs")

	var req cdsapi.ObjectListByIDs
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	objs, err := s.svc.ObjectSvc().GetByIDs(req.UserID, req.ObjectIDs)
	if err != nil {
		log.Warnf("listing objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("listing objects: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectListByIDsResp{Objects: objs}))
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

	up, err := s.svc.Uploader.BeginUpdate(req.Info.UserID, req.Info.PackageID, req.Info.Affinity, req.Info.LoadTo, req.Info.LoadToPath)
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
	if file.File == nil {
		log.Warnf("object not found: %d", req.ObjectID)
		ctx.JSON(http.StatusOK, Failed(errorcode.DataNotFound, "object not found"))
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
		s.svc.AccessStat.AddAccessCounter(file.Object.ObjectID, file.Object.PackageID, config.Cfg().StorageID, math2.DivOrDefault(float64(n), float64(file.Object.Size), 1))
	}
}

func (s *ObjectService) DownloadByPath(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.DownloadByPath")

	var req cdsapi.ObjectDownloadByPath
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	obj, err := s.svc.ObjectSvc().GetByPath(req.UserID, req.PackageID, req.Path, false)
	if err != nil {
		log.Warnf("getting object by path: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get object by path failed"))
		return
	}

	if len(obj) == 0 {
		log.Warnf("object not found: %s", req.Path)
		ctx.JSON(http.StatusOK, Failed(errorcode.DataNotFound, "object not found"))
		return
	}

	off := req.Offset
	len := int64(-1)
	if req.Length != nil {
		len = *req.Length
	}

	file, err := s.svc.ObjectSvc().Download(req.UserID, downloader.DownloadReqeust{
		ObjectID: obj[0].ObjectID,
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

	if config.Cfg().StorageID > 0 {
		s.svc.AccessStat.AddAccessCounter(file.Object.ObjectID, file.Object.PackageID, config.Cfg().StorageID, math2.DivOrDefault(float64(n), float64(file.Object.Size), 1))
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

func (s *ObjectService) UpdateInfoByPath(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.UpdateInfoByPath")

	var req cdsapi.ObjectUpdateInfoByPath
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	obj, err := s.svc.ObjectSvc().GetByPath(req.UserID, req.PackageID, req.Path, true)
	if err != nil {
		log.Warnf("getting object by path: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get object by path failed"))
		return
	}
	if len(obj) == 0 {
		log.Warnf("object not found: %s", req.Path)
		ctx.JSON(http.StatusOK, Failed(errorcode.DataNotFound, "object not found"))
		return
	}

	sucs, err := s.svc.ObjectSvc().UpdateInfo(req.UserID, []cdsapi.UpdatingObject{{
		ObjectID:   obj[0].ObjectID,
		UpdateTime: req.UpdateTime,
	}})
	if err != nil {
		log.Warnf("updating objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "update objects failed"))
		return
	}
	if len(sucs) == 0 {
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectUpdateInfoByPathResp{}))
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

func (s *ObjectService) DeleteByPath(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.DeleteByPath")

	var req cdsapi.ObjectDeleteByPath
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	obj, err := s.svc.ObjectSvc().GetByPath(req.UserID, req.PackageID, req.Path, false)
	if err != nil {
		log.Warnf("getting object by path: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get object by path failed"))
		return
	}
	if len(obj) == 0 {
		ctx.JSON(http.StatusOK, OK(nil))
		return
	}

	err = s.svc.ObjectSvc().Delete(req.UserID, []cdssdk.ObjectID{obj[0].ObjectID})
	if err != nil {
		log.Warnf("deleting objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "delete objects failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}

func (s *ObjectService) Clone(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Clone")

	var req cdsapi.ObjectClone
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	objs, err := s.svc.ObjectSvc().Clone(req.UserID, req.Clonings)
	if err != nil {
		log.Warnf("cloning object: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "clone object failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectCloneResp{Objects: objs}))
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
