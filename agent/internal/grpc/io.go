package grpc

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	agentserver "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

// SendStream 接收客户端通过流式传输发送的文件数据。
//
// server: 代表服务端发送流的接口，用于接收和响应客户端请求。
// 返回值: 返回错误信息，如果处理成功则返回nil。
func (s *Service) SendStream(server agentserver.Agent_SendStreamServer) error {
	// 接收流式传输的初始化信息包
	msg, err := server.Recv()
	if err != nil {
		return fmt.Errorf("recving stream id packet: %w", err)
	}
	// 校验初始化信息包类型
	if msg.Type != agentserver.StreamDataPacketType_SendArgs {
		return fmt.Errorf("first packet must be a SendArgs packet")
	}

	logger.
		WithField("PlanID", msg.PlanID).
		WithField("StreamID", msg.StreamID).
		Debugf("receive stream from grpc")

	pr, pw := io.Pipe()

	// 通知系统，流式传输已准备就绪
	s.sw.StreamReady(ioswitch.PlanID(msg.PlanID), ioswitch.NewStream(ioswitch.StreamID(msg.StreamID), pr))

	// 循环接收客户端发送的文件数据
	var recvSize int64
	for {
		msg, err := server.Recv()

		// 处理接收数据错误
		if err != nil {
			pw.CloseWithError(io.ErrClosedPipe)
			logger.WithField("ReceiveSize", recvSize).
				Warnf("recv message failed, err: %s", err.Error())
			return fmt.Errorf("recv message failed, err: %w", err)
		}

		// 将接收到的数据写入管道
		err = myio.WriteAll(pw, msg.Data)
		if err != nil {
			pw.CloseWithError(io.ErrClosedPipe)
			logger.Warnf("write data to file failed, err: %s", err.Error())
			return fmt.Errorf("write data to file failed, err: %w", err)
		}

		recvSize += int64(len(msg.Data))

		// 当接收到EOF信息时，结束写入并返回
		if msg.Type == agentserver.StreamDataPacketType_EOF {
			err := pw.Close()
			if err != nil {
				logger.Warnf("finish writing failed, err: %s", err.Error())
				return fmt.Errorf("finish writing failed, err: %w", err)
			}

			// 向客户端发送传输完成的响应
			err = server.SendAndClose(&agentserver.SendStreamResp{})
			if err != nil {
				logger.Warnf("send response failed, err: %s", err.Error())
				return fmt.Errorf("send response failed, err: %w", err)
			}

			return nil
		}
	}
}

// FetchStream 从服务端获取流式数据并发送给客户端。
//
// req: 包含获取流式数据所需的计划ID和流ID的请求信息。
// server: 用于向客户端发送流数据的服务器接口。
// 返回值: 返回处理过程中出现的任何错误。
func (s *Service) FetchStream(req *agentserver.FetchStreamReq, server agentserver.Agent_FetchStreamServer) error {
	logger.
		WithField("PlanID", req.PlanID).
		WithField("StreamID", req.StreamID).
		Debugf("send stream by grpc")

	// 等待对应的流数据准备就绪
	strs, err := s.sw.WaitStreams(ioswitch.PlanID(req.PlanID), ioswitch.StreamID(req.StreamID))
	if err != nil {
		logger.
			WithField("PlanID", req.PlanID).
			WithField("StreamID", req.StreamID).
			Warnf("watting stream: %s", err.Error())
		return fmt.Errorf("watting stream: %w", err)
	}

	reader := strs[0].Stream
	defer reader.Close()

	// 读取流数据并发送给客户端
	buf := make([]byte, 4096)
	readAllCnt := 0
	for {
		readCnt, err := reader.Read(buf)

		if readCnt > 0 {
			readAllCnt += readCnt
			err = server.Send(&agentserver.StreamDataPacket{
				Type: agentserver.StreamDataPacketType_Data,
				Data: buf[:readCnt],
			})
			if err != nil {
				logger.
					WithField("PlanID", req.PlanID).
					WithField("StreamID", req.StreamID).
					Warnf("send stream data failed, err: %s", err.Error())
				return fmt.Errorf("send stream data failed, err: %w", err)
			}
		}

		// 当读取完毕或遇到EOF时返回
		if err == io.EOF {
			logger.
				WithField("PlanID", req.PlanID).
				WithField("StreamID", req.StreamID).
				Debugf("send data size %d", readAllCnt)
			// 发送EOF消息通知客户端数据传输完成
			server.Send(&agentserver.StreamDataPacket{
				Type: agentserver.StreamDataPacketType_EOF,
			})
			return nil
		}

		// 处理除EOF和io.ErrUnexpectedEOF之外的读取错误
		if err != nil && err != io.ErrUnexpectedEOF {
			logger.
				WithField("PlanID", req.PlanID).
				WithField("StreamID", req.StreamID).
				Warnf("reading stream data: %s", err.Error())
			return fmt.Errorf("reading stream data: %w", err)
		}
	}
}
