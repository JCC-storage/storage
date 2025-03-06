package http

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/client/internal/config"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
)

type PresignedService struct {
	*Server
}

func (s *Server) Presigned() *PresignedService {
	return &PresignedService{
		Server: s,
	}
}

func (s *PresignedService) ObjectDownloadByPath(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Presigned.ObjectDownloadByPath")

	var req cdsapi.PresignedObjectDownloadByPath
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	_, obj, err := s.svc.ObjectSvc().GetByPath(req.UserID, req.PackageID, req.Path, false, false)
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

func (s *PresignedService) ObjectUpload(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Presigned.ObjectUpload")

	var req cdsapi.PresignedObjectUpload
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	up, err := s.svc.Uploader.BeginUpdate(req.UserID, req.PackageID, req.Affinity, req.LoadTo, req.LoadToPath)
	if err != nil {
		log.Warnf("begin update: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("begin update: %v", err)))
		return
	}
	defer up.Abort()

	path := filepath.ToSlash(req.Path)

	err = up.Upload(path, ctx.Request.Body)
	if err != nil {
		log.Warnf("uploading file: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("uploading file %v: %v", req.Path, err)))
		return
	}

	ret, err := up.Commit()
	if err != nil {
		log.Warnf("commit update: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("commit update: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PresignedObjectUploadResp{Object: ret.Objects[path]}))
}

func (s *PresignedService) ObjectNewMultipartUpload(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Presigned.ObjectNewMultipartUpload")

	var req cdsapi.PresignedObjectNewMultipartUpload
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	obj, err := s.svc.ObjectSvc().NewMultipartUploadObject(req.UserID, req.PackageID, req.Path)
	if err != nil {
		log.Warnf("new multipart upload: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("new multipart upload: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.PresignedObjectUploadResp{Object: obj}))
}

func (s *PresignedService) ObjectUploadPart(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Presigned.ObjectUploadPart")

	var req cdsapi.PresignedObjectUploadPart
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	err := s.svc.Uploader.UploadPart(req.UserID, req.ObjectID, req.Index, ctx.Request.Body)
	if err != nil {
		log.Warnf("uploading part: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("upload part: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectUploadPartResp{}))
}

func (s *PresignedService) ObjectCompleteMultipartUpload(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Presigned.ObjectCompleteMultipartUpload")

	var req cdsapi.PresignedObjectCompleteMultipartUpload
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	obj, err := s.svc.ObjectSvc().CompleteMultipartUpload(req.UserID, req.ObjectID, req.Indexes)
	if err != nil {
		log.Warnf("completing multipart upload: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("complete multipart upload: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdsapi.ObjectCompleteMultipartUploadResp{Object: obj}))
}
