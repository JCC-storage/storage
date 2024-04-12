package grpc

import (
	"fmt"
	"io"

	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/io2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agentserver "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

// Service 类定义了与agent服务相关的操作
type Service struct {
	agentserver.AgentServer
	sw *ioswitch.Switch
}

// NewService 创建并返回一个新的Service实例
func NewService(sw *ioswitch.Switch) *Service {
	return &Service{
		sw: sw,
	}
}

// SendIPFSFile 处理客户端上传文件到IPFS的请求
func (s *Service) SendIPFSFile(server agentserver.Agent_SendIPFSFileServer) error {
	log.Debugf("client upload file")

	ipfsCli, err := stgglb.IPFSPool.Acquire() // 获取一个IPFS客户端实例
	if err != nil {
		log.Warnf("new ipfs client: %s", err.Error())
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close()

	writer, err := ipfsCli.CreateFileStream() // 在IPFS上创建一个文件流
	if err != nil {
		log.Warnf("create file failed, err: %s", err.Error())
		return fmt.Errorf("create file failed, err: %w", err)
	}

	var recvSize int64
	for {
		msg, err := server.Recv() // 接收客户端发送的文件数据

		if err != nil {
			writer.Abort(io.ErrClosedPipe) // 出错时关闭文件写入
			log.WithField("ReceiveSize", recvSize).
				Warnf("recv message failed, err: %s", err.Error())
			return fmt.Errorf("recv message failed, err: %w", err)
		}

		err = io2.WriteAll(writer, msg.Data)
		if err != nil {
			writer.Abort(io.ErrClosedPipe) // 写入出错时关闭文件写入
			log.Warnf("write data to file failed, err: %s", err.Error())
			return fmt.Errorf("write data to file failed, err: %w", err)
		}

		recvSize += int64(len(msg.Data))

		if msg.Type == agentserver.StreamDataPacketType_EOF { // 当接收到EOF标志时，结束文件写入并返回文件Hash
			hash, err := writer.Finish()
			if err != nil {
				log.Warnf("finish writing failed, err: %s", err.Error())
				return fmt.Errorf("finish writing failed, err: %w", err)
			}

			err = server.SendAndClose(&agentserver.SendIPFSFileResp{
				FileHash: hash,
			})
			if err != nil {
				log.Warnf("send response failed, err: %s", err.Error())
				return fmt.Errorf("send response failed, err: %w", err)
			}

			log.Debugf("%d bytes received ", recvSize)
			return nil
		}
	}
}

// GetIPFSFile 处理客户端从IPFS下载文件的请求
func (s *Service) GetIPFSFile(req *agentserver.GetIPFSFileReq, server agentserver.Agent_GetIPFSFileServer) error {
	log.WithField("FileHash", req.FileHash).Debugf("client download file")

	ipfsCli, err := stgglb.IPFSPool.Acquire() // 获取一个IPFS客户端实例
	if err != nil {
		log.Warnf("new ipfs client: %s", err.Error())
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close()

	reader, err := ipfsCli.OpenRead(req.FileHash) // 通过文件Hash打开一个读取流
	if err != nil {
		log.Warnf("open file %s to read failed, err: %s", req.FileHash, err.Error())
		return fmt.Errorf("open file to read failed, err: %w", err)
	}
	defer reader.Close()

	buf := make([]byte, 1024)
	readAllCnt := 0
	for {
		readCnt, err := reader.Read(buf) // 从IPFS读取数据

		if readCnt > 0 {
			readAllCnt += readCnt
			err = server.Send(&agentserver.FileDataPacket{
				Type: agentserver.StreamDataPacketType_Data,
				Data: buf[:readCnt],
			})
			if err != nil {
				log.WithField("FileHash", req.FileHash).
					Warnf("send file data failed, err: %s", err.Error())
				return fmt.Errorf("send file data failed, err: %w", err)
			}
		}

		if err == io.EOF { // 当读取完毕时，发送EOF标志并返回
			log.WithField("FileHash", req.FileHash).Debugf("send data size %d", readAllCnt)
			server.Send(&agentserver.FileDataPacket{
				Type: agentserver.StreamDataPacketType_EOF,
			})
			return nil
		}

		if err != nil && err != io.ErrUnexpectedEOF { // 遇到除EOF和ErrUnexpectedEOF外的其他错误，中断操作
			log.Warnf("read file %s data failed, err: %s", req.FileHash, err.Error())
			return fmt.Errorf("read file data failed, err: %w", err)
		}
	}
}
