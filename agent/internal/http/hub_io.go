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
	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	"gitlink.org.cn/cloudream/common/utils/http2"
	"gitlink.org.cn/cloudream/common/utils/io2"
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

	ctx.Header("Content-Type", http2.ContentTypeOctetStream)

	startTime := time.Now()

	cw := http2.NewChunkedWriter(io2.NopWriteCloser(ctx.Writer))
	n, err := cw.WriteStreamPart("stream", strVal.Stream)
	dt := time.Since(startTime)
	log.Debugf("size: %v, time: %v, speed: %v/s", n, dt, bytesize.New(float64(n)/dt.Seconds()))

	if err != nil {
		log.Warnf("writing stream part: %v", err)
		cw.Abort(err.Error())
		return
	}

	err = cw.Finish()
	if err != nil {
		log.Warnf("finishing chunked writer: %v", err)
		return
	}
}

func (s *IOService) SendStream(ctx *gin.Context) {
	cr := http2.NewChunkedReader(ctx.Request.Body)
	_, infoData, err := cr.NextDataPart()
	if err != nil {
		logger.Warnf("reading info data: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.OperationFailed, fmt.Sprintf("reading info data: %v", err)))
		return
	}

	info, err := serder.JSONToObjectEx[cdsapi.SendStreamInfo](infoData)
	if err != nil {
		logger.Warnf("deserializing info data: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, fmt.Sprintf("deserializing info data: %v", err)))
		return
	}

	_, stream, err := cr.NextPart()
	if err != nil {
		logger.Warnf("reading stream data: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.OperationFailed, fmt.Sprintf("reading stream data: %v", err)))
		return
	}
	defer cr.Close()

	// 超时设置
	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	sw := s.svc.swWorker.FindByIDContexted(c, info.PlanID)
	if sw == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}

	fut := future.NewSetVoid()
	sw.PutVar(info.VarID, &exec.StreamValue{
		Stream: io2.DelegateReadCloser(stream, func() error {
			fut.SetVoid()
			return nil
		}),
	})

	// 等待流发送完毕才能发送响应
	err = fut.Wait(ctx.Request.Context())
	if err != nil {
		logger.Warnf("sending stream: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, fmt.Sprintf("sending stream: %v", err)))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
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
	exec.SetValueByType(execCtx, s.svc.stgMgr)
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
