package http

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/inhies/go-bytesize"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

type IOService struct {
	*Server
}

func (s *Server) IOSvc() *IOService {
	return &IOService{
		Server: s,
	}
}

func (s *IOService) GetStream(ctx *gin.Context) {
	log := logger.WithField("HTTP", "HubIO.GetStream")

	req, err := serder.JSONToObjectStreamEx[cdsapi.GetStreamReq](ctx.Request.Body)
	if err != nil {
		log.Warnf("deserializing request: %v", err)
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	log = log.WithField("PlanID", req.PlanID).WithField("VarID", req.VarID)

	// 设置超时
	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	sw := s.svc.swWorker.FindByIDContexted(c, req.PlanID)
	if sw == nil {
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "plan not found"))
		return
	}

	sw.PutVar(req.SignalID, req.Signal)

	strVal, err := exec.BindVar[*exec.StreamValue](sw, ctx.Request.Context(), req.VarID)
	if err != nil {
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("bind var: %v", err)))
		return
	}
	defer strVal.Stream.Close()

	ctx.Header("Content-Type", "application/octet-stream")

	startTime := time.Now()
	n, err := cdsapi.WriteStream(ctx.Writer, strVal.Stream)
	if err != nil {
		log.Warnf("sending stream: %v", err)
		return
	}
	dt := time.Since(startTime)

	log.Debugf("send stream completed, size: %v, time: %v, speed: %v/s", n, dt, bytesize.New(float64(n)/dt.Seconds()))
}

func (s *IOService) SendStream(ctx *gin.Context) {
	ctx.JSON(http.StatusBadRequest, Failed(errorcode.OperationFailed, "not implemented"))
	return

	// var req cdsapi.SendStreamReq
	// if err := ctx.ShouldBindJSON(&req); err != nil {
	// 	logger.Warnf("binding body: %s", err.Error())
	// 	ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
	// 	return
	// }

	// logger.
	// 	WithField("PlanID", req.PlanID).
	// 	WithField("VarID", req.VarID).
	// 	Debugf("stream input")

	// 超时设置
	// c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	// defer cancel()

	// sw := s.svc.swWorker.FindByIDContexted(c, req.PlanID)
	// if sw == nil {
	// 	ctx.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
	// 	return
	// }

	// pr, pw := io.Pipe()
	// defer pr.Close()

	// streamVar := &exec.StreamVar{
	// 	ID:     req.VarID,
	// 	Stream: pr,
	// }
	// sw.PutVar(streamVar)

	// var recvSize int64

	// go func() {
	// 	defer pw.Close()
	// 	_, err := io.Copy(pw, ctx.Request.Body)
	// 	if err != nil {
	// 		logger.Warnf("write data to file failed, err: %s", err.Error())
	// 		pw.CloseWithError(fmt.Errorf("write data to file failed: %w", err))
	// 	}
	// }()

	// for {
	// 	buf := make([]byte, 1024*64)
	// 	n, err := pr.Read(buf)
	// 	if err != nil {
	// 		if err == io.EOF {
	// 			logger.WithField("ReceiveSize", recvSize).
	// 				WithField("VarID", req.VarID).
	// 				Debugf("file transmission completed")

	// 			// 将结果返回给客户端
	// 			ctx.JSON(http.StatusOK, gin.H{"message": "file transmission completed"})
	// 			return
	// 		}
	// 		logger.Warnf("read stream failed, err: %s", err.Error())
	// 		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("read stream failed: %v", err)})
	// 		return
	// 	}

	// 	if n > 0 {
	// 		recvSize += int64(n)
	// 		// 处理接收到的数据，例如写入文件或进行其他操作
	// 	}
	// }
}

func (s *IOService) ExecuteIOPlan(ctx *gin.Context) {
	log := logger.WithField("HTTP", "HubIO.ExecuteIOPlan")

	data, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		log.Warnf("reading body: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "reading body failed"))
		return
	}

	req, err := serder.JSONToObjectEx[cdsapi.ExecuteIOPlanReq](data)
	if err != nil {
		log.Warnf("deserializing request: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	log = log.WithField("PlanID", req.Plan.ID)

	log.Infof("begin execute io plan")

	sw := exec.NewExecutor(req.Plan)

	s.svc.swWorker.Add(sw)
	defer s.svc.swWorker.Remove(sw)

	execCtx := exec.NewWithContext(ctx.Request.Context())

	// TODO 注入依赖
	_, err = sw.Run(execCtx)
	if err != nil {
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("executing plan: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}

func (s *IOService) SendVar(ctx *gin.Context) {
	log := logger.WithField("HTTP", "HubIO.SendVar")

	data, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		log.Warnf("reading body: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "reading body failed"))
		return
	}

	req, err := serder.JSONToObjectEx[cdsapi.SendVarReq](data)
	if err != nil {
		log.Warnf("deserializing request: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	sw := s.svc.swWorker.FindByIDContexted(c, req.PlanID)
	if sw == nil {
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "plan not found"))
		return
	}

	sw.PutVar(req.VarID, req.VarValue)

	ctx.JSON(http.StatusOK, OK(nil))
}

func (s *IOService) GetVar(ctx *gin.Context) {
	log := logger.WithField("HTTP", "HubIO.GetVar")

	data, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		log.Warnf("reading body: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "reading body failed"))
		return
	}

	req, err := serder.JSONToObjectEx[cdsapi.GetVarReq](data)
	if err != nil {
		log.Warnf("deserializing request: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	log = log.WithField("PlanID", req.PlanID).WithField("VarID", req.VarID)

	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	sw := s.svc.swWorker.FindByIDContexted(c, req.PlanID)
	if sw == nil {
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "plan not found"))
		return
	}

	sw.PutVar(req.SignalID, req.Signal)

	v, err := sw.BindVar(ctx.Request.Context(), req.VarID)
	if err != nil {
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("bind var: %v", err)))
		return
	}

	resp := Response{
		Code: errorcode.OK,
		Data: cdsapi.GetVarResp{
			Value: v,
		},
	}

	respData, err := serder.ObjectToJSONEx(resp)
	if err != nil {
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("serializing response: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, respData)
}
