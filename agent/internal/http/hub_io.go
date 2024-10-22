package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
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
	var req cdsapi.GetStreamReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		logger.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	logger.
		WithField("PlanID", req.PlanID).
		WithField("VarID", req.VarID).
		Debugf("stream output")

	// 设置超时
	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	sw := s.svc.swWorker.FindByIDContexted(c, req.PlanID)
	if sw == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}

	signalBytes, err := serder.ObjectToJSON(req.Signal)
	if err != nil {
		logger.Warnf("serializing SignalVar: %s", err)
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "serializing SignalVar fail"))
		return
	}
	signal, err := serder.JSONToObjectEx[*exec.SignalVar](signalBytes)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("deserializing var: %v", err)})
		return
	}

	sw.PutVars(signal)

	strVar := &exec.StreamVar{
		ID: req.VarID,
	}
	err = sw.BindVars(ctx.Request.Context(), strVar)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("binding vars: %v", err)})
		return
	}

	reader := strVar.Stream
	defer reader.Close()

	ctx.Header("Content-Type", "application/octet-stream")
	ctx.Status(http.StatusOK)

	buf := make([]byte, 1024*64)
	readAllCnt := 0
	startTime := time.Now()
	for {
		readCnt, err := reader.Read(buf)

		if readCnt > 0 {
			readAllCnt += readCnt
			_, err := ctx.Writer.Write(buf[:readCnt])
			if err != nil {
				logger.
					WithField("PlanID", req.PlanID).
					WithField("VarID", req.VarID).
					Warnf("send stream data failed, err: %s", err.Error())
				ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("send stream data failed, err: %v", err)})
				return
			}
			// 刷新缓冲区，确保数据立即发送
			ctx.Writer.Flush()
		}

		// 文件读取完毕
		if err == io.EOF {
			dt := time.Since(startTime)
			logger.
				WithField("PlanID", req.PlanID).
				WithField("VarID", req.VarID).
				Debugf("send data size %d in %v, speed %v/s", readAllCnt, dt, bytesize.New(float64(readAllCnt)/dt.Seconds()))
			return
		}

		// io.ErrUnexpectedEOF 没有读满整个 buf 就遇到了 EOF，此时正常发送剩余数据即可
		if err != nil && err != io.ErrUnexpectedEOF {
			logger.
				WithField("PlanID", req.PlanID).
				WithField("VarID", req.VarID).
				Warnf("reading stream data: %s", err.Error())
			ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("reading stream data: %v", err)})
			return
		}
	}
}

func (s *IOService) SendStream(ctx *gin.Context) {
	//planID := ctx.PostForm("plan_id")
	//varID := ctx.PostForm("var_id")

	var req cdsapi.SendStreamReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		logger.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	logger.
		WithField("PlanID", req.PlanID).
		WithField("VarID", req.VarID).
		Debugf("stream input")

	// 超时设置
	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	sw := s.svc.swWorker.FindByIDContexted(c, req.PlanID)
	if sw == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}

	pr, pw := io.Pipe()
	defer pr.Close()

	streamVar := &exec.StreamVar{
		ID:     req.VarID,
		Stream: pr,
	}
	sw.PutVars(streamVar)

	var recvSize int64

	go func() {
		defer pw.Close()
		_, err := io.Copy(pw, ctx.Request.Body)
		if err != nil {
			logger.Warnf("write data to file failed, err: %s", err.Error())
			pw.CloseWithError(fmt.Errorf("write data to file failed: %w", err))
		}
	}()

	for {
		buf := make([]byte, 1024*64)
		n, err := pr.Read(buf)
		if err != nil {
			if err == io.EOF {
				logger.WithField("ReceiveSize", recvSize).
					WithField("VarID", req.VarID).
					Debugf("file transmission completed")

				// 将结果返回给客户端
				ctx.JSON(http.StatusOK, gin.H{"message": "file transmission completed"})
				return
			}
			logger.Warnf("read stream failed, err: %s", err.Error())
			ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("read stream failed: %v", err)})
			return
		}

		if n > 0 {
			recvSize += int64(n)
			// 处理接收到的数据，例如写入文件或进行其他操作
		}
	}
}

func (s *IOService) ExecuteIOPlan(ctx *gin.Context) {
	bodyBytes, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		logger.Warnf("reading body: %s", err.Error())
		ctx.JSON(http.StatusInternalServerError, Failed("400", "internal error"))
		return
	}
	println("Received body: %s", string(bodyBytes))
	ctx.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // Reset body for subsequent reads

	var req cdsapi.ExecuteIOPlanReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		logger.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	planBytes, err := serder.ObjectToJSON(req.Plan)
	// 反序列化 Plan
	plan, err := serder.JSONToObjectEx[exec.Plan](planBytes)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("deserializing plan: %v", err)})
		return
	}

	logger.WithField("PlanID", plan.ID).Infof("begin execute io plan")
	defer logger.WithField("PlanID", plan.ID).Infof("plan finished")

	sw := exec.NewExecutor(plan)

	s.svc.swWorker.Add(sw)
	defer s.svc.swWorker.Remove(sw)

	// 设置上下文超时
	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	_, err = sw.Run(c)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("running io plan: %v", err)})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "plan executed successfully"})
}

func (s *IOService) SendVar(ctx *gin.Context) {
	var req cdsapi.SendVarReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		logger.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	sw := s.svc.swWorker.FindByIDContexted(c, req.PlanID)
	if sw == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}

	VarBytes, err := serder.ObjectToJSON(req.Var)
	v, err := serder.JSONToObjectEx[exec.Var](VarBytes)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("deserializing var: %v", err)})
		return
	}

	sw.PutVars(v)
	ctx.JSON(http.StatusOK, gin.H{"message": "var sent successfully"})
}

func (s *IOService) GetVar(ctx *gin.Context) {
	var req cdsapi.GetVarReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		logger.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	c, cancel := context.WithTimeout(ctx.Request.Context(), time.Second*30)
	defer cancel()

	sw := s.svc.swWorker.FindByIDContexted(c, req.PlanID)
	if sw == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}

	VarBytes, err := serder.ObjectToJSON(req.Var)
	v, err := serder.JSONToObjectEx[exec.Var](VarBytes)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("deserializing var: %v", err)})
		return
	}

	SignalBytes, err := serder.ObjectToJSON(req.Signal)
	signal, err := serder.JSONToObjectEx[*exec.SignalVar](SignalBytes)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("deserializing signal: %v", err)})
		return
	}

	sw.PutVars(signal)

	err = sw.BindVars(c, v)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("binding vars: %v", err)})
		return
	}

	vd, err := serder.ObjectToJSONEx(v)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("serializing var: %v", err)})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"var": string(vd)})
}
