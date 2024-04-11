package http

import (
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	myio "gitlink.org.cn/cloudream/common/utils/io"
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
		complete, _, err := s.svc.ObjectSvc().WaitUploading(taskID, time.Second*5)
		if complete {
			if err != nil {
				log.Warnf("uploading object: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "uploading object failed"))
				return
			}
			ctx.JSON(http.StatusOK, OK(nil))
			return
		}

		if err != nil {
			log.Warnf("waiting task: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "wait uploading task failed"))
			return
		}
	}
}

// ObjectDownloadReq 定义下载对象请求的结构体
type ObjectDownloadReq struct {
	UserID   *cdssdk.UserID   `form:"userID" binding:"required"`   // 用户ID
	ObjectID *cdssdk.ObjectID `form:"objectID" binding:"required"` // 对象ID
}

// Download 处理对象下载请求
func (s *ObjectService) Download(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Download")

	var req ObjectDownloadReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	// 下载对象
	file, err := s.svc.ObjectSvc().Download(*req.UserID, *req.ObjectID)
	if err != nil {
		log.Warnf("downloading object: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "download object failed"))
		return
	}

	// 设置响应头，进行文件下载
	ctx.Writer.WriteHeader(http.StatusOK)
	ctx.Header("Content-Disposition", "attachment; filename=filename")
	ctx.Header("Content-Type", "application/octet-stream")

	// 通过流式传输返回文件内容
	buf := make([]byte, 4096)
	ctx.Stream(func(w io.Writer) bool {
		rd, err := file.Read(buf)
		if err == io.EOF {
			err = myio.WriteAll(w, buf[:rd])
			if err != nil {
				log.Warnf("writing data to response: %s", err.Error())
			}
			return false
		}

		if err != nil {
			log.Warnf("reading file data: %s", err.Error())
			return false
		}

		err = myio.WriteAll(w, buf[:rd])
		if err != nil {
			log.Warnf("writing data to response: %s", err.Error())
			return false
		}

		return true
	})
}

// GetPackageObjectsReq 定义获取包内对象请求的结构体
type GetPackageObjectsReq struct {
	UserID    *cdssdk.UserID    `form:"userID" binding:"required"`    // 用户ID
	PackageID *cdssdk.PackageID `form:"packageID" binding:"required"` // 包ID
}

// GetPackageObjectsResp 定义获取包内对象响应的结构体
type GetPackageObjectsResp = cdssdk.ObjectGetPackageObjectsResp

// GetPackageObjects 处理获取包内对象的请求
func (s *ObjectService) GetPackageObjects(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.GetPackageObjects")

	var req GetPackageObjectsReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	// 获取包内的对象列表
	objs, err := s.svc.ObjectSvc().GetPackageObjects(*req.UserID, *req.PackageID)
	if err != nil {
		log.Warnf("getting package objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package object failed"))
		return
	}

	// 返回响应
	ctx.JSON(http.StatusOK, OK(GetPackageObjectsResp{Objects: objs}))
}
