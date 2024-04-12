package http

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	myhttp "gitlink.org.cn/cloudream/common/utils/http"
)

// ObjectService 服务结构体，处理对象相关的HTTP请求
type ObjectService struct {
	*Server
}

// Object 返回ObjectService的实例
func (s *Server) Object() *ObjectService {
	return &ObjectService{
		Server: s,
	}
}

// ObjectUploadReq 定义上传对象请求的结构体
type ObjectUploadReq struct {
	Info  cdssdk.ObjectUploadInfo `form:"info" binding:"required"` // 上传信息
	Files []*multipart.FileHeader `form:"files"`                   // 上传文件列表
}

// Upload 处理对象上传请求
func (s *ObjectService) Upload(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Upload")

	var req ObjectUploadReq
	if err := ctx.ShouldBind(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	// 将multipart文件转换为上传对象
	objIter := mapMultiPartFileToUploadingObject(req.Files)

	// 开始上传任务
	taskID, err := s.svc.ObjectSvc().StartUploading(req.Info.UserID, req.Info.PackageID, objIter, req.Info.NodeAffinity)
	if err != nil {
		log.Warnf("start uploading object task: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "start uploading task failed"))
		return
	}

	// 等待上传任务完成
	for {
		complete, objs, err := s.svc.ObjectSvc().WaitUploading(taskID, time.Second*5)
		if complete {
			if err != nil {
				log.Warnf("uploading object: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "uploading object failed"))
				return
			}

			uploadeds := make([]cdssdk.UploadedObject, len(objs.Objects))
			for i, obj := range objs.Objects {
				err := ""
				if obj.Error != nil {
					err = obj.Error.Error()
				}
				o := obj.Object
				uploadeds[i] = cdssdk.UploadedObject{
					Object: &o,
					Error:  err,
				}
			}

			ctx.JSON(http.StatusOK, OK(cdssdk.ObjectUploadResp{Uploadeds: uploadeds}))
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

	var req cdssdk.ObjectDownload
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	file, err := s.svc.ObjectSvc().Download(req.UserID, req.ObjectID)
	if err != nil {
		log.Warnf("downloading object: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "download object failed"))
		return
	}

	mw := multipart.NewWriter(ctx.Writer)
	defer mw.Close()

	ctx.Writer.Header().Set("Content-Type", fmt.Sprintf("%s;boundary=%s", myhttp.ContentTypeMultiPart, mw.Boundary()))
	ctx.Writer.WriteHeader(http.StatusOK)

	fw, err := mw.CreateFormFile("file", path.Base(file.Object.Path))
	if err != nil {
		log.Warnf("creating form file: %s", err.Error())
		return
	}

	io.Copy(fw, file.File)
}

func (s *ObjectService) UpdateInfo(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.UpdateInfo")

	var req cdssdk.ObjectUpdateInfo
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

	ctx.JSON(http.StatusOK, OK(cdssdk.ObjectUpdateInfoResp{Successes: sucs}))
}

func (s *ObjectService) Move(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Move")

	var req cdssdk.ObjectMove
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

	ctx.JSON(http.StatusOK, OK(cdssdk.ObjectMoveResp{Successes: sucs}))
}

func (s *ObjectService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Delete")

	var req cdssdk.ObjectDelete
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

// GetPackageObjects 处理获取包内对象的请求
func (s *ObjectService) GetPackageObjects(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.GetPackageObjects")

	var req cdssdk.ObjectGetPackageObjects
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

	ctx.JSON(http.StatusOK, OK(cdssdk.ObjectGetPackageObjectsResp{Objects: objs}))
}
