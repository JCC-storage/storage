package http

import (
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
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

	var err error

	objIter := mapMultiPartFileToUploadingObject(req.Files)

	taskID, err := s.svc.ObjectSvc().StartUploading(req.Info.UserID, req.Info.PackageID, objIter, req.Info.NodeAffinity)

	if err != nil {
		log.Warnf("start uploading object task: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "start uploading task failed"))
		return
	}

	for {
		complete, objs, err := s.svc.ObjectSvc().WaitUploading(taskID, time.Second*5)
		if complete {
			if err != nil {
				log.Warnf("uploading object: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "uploading object failed"))
				return
			}

			uploadeds := make([]cdsapi.UploadedObject, len(objs.Objects))
			for i, obj := range objs.Objects {
				err := ""
				if obj.Error != nil {
					err = obj.Error.Error()
				}
				o := obj.Object
				uploadeds[i] = cdsapi.UploadedObject{
					Object: &o,
					Error:  err,
				}
			}

			ctx.JSON(http.StatusOK, OK(cdsapi.ObjectUploadResp{Uploadeds: uploadeds}))
			return
		}

		if err != nil {
			log.Warnf("waiting task: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "wait uploading task failed"))
			return
		}
	}
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
	if stgglb.Local.NodeID != nil {
		s.svc.AccessStat.AddAccessCounter(file.Object.ObjectID, file.Object.PackageID, *stgglb.Local.NodeID, float64(n)/float64(file.Object.Size))
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
