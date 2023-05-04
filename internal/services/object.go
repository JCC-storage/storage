package services

import (
	"fmt"
	"io"
	"math/rand"
	"sync"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/db/model"
	agentcaller "gitlink.org.cn/cloudream/proto"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
	mygrpc "gitlink.org.cn/cloudream/utils/grpc"
	myio "gitlink.org.cn/cloudream/utils/io"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	lo "github.com/samber/lo"
)

type ObjectService struct {
	*Service
}

func ObjectSvc(svc *Service) *ObjectService {
	return &ObjectService{Service: svc}
}

func (svc *ObjectService) GetObject(userID int, objectID int) (model.Object, error) {
	// TODO
	panic("not implement yet")
}

func (svc *ObjectService) DownloadObject(userID int, objectID int) (io.ReadCloser, error) {
	readResp, err := svc.coordinator.Read(coormsg.NewReadCommandBody(objectID, userID, config.Cfg().ExternalIP))
	if err != nil {
		return nil, fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if readResp.ErrorCode != errorcode.OK {
		return nil, fmt.Errorf("coordinator operation failed, code: %s, message: %s", readResp.ErrorCode, readResp.ErrorMessage)
	}

	switch readResp.Body.Redundancy {
	case consts.REDUNDANCY_REP:
		if len(readResp.Body.Entries) == 0 {
			return nil, fmt.Errorf("no node has this file")
		}

		// 选择下载节点
		entry := svc.chooseDownloadNode(readResp.Body.Entries)

		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := entry.NodeExternalIP
		if entry.IsSameLocation {
			nodeIP = entry.NodeLocalIP
			// TODO 以后考虑用log
			fmt.Printf("client and node %d are at the same location, use local ip\n", entry.NodeID)
		}

		reader, err := svc.downloadAsRepObject(nodeIP, entry.FileHash)
		if err != nil {
			return nil, fmt.Errorf("rep read failed, err: %w", err)
		}

		return reader, nil

		//case consts.REDUNDANCY_EC:
		// TODO EC部分的代码要考虑重构
		//	ecRead(readResp.FileSizeInBytes, readResp.NodeIPs, readResp.Hashes, readResp.BlockIDs, *readResp.ECName)
	}

	return nil, fmt.Errorf("unsupported redundancy type: %s", readResp.Body.Redundancy)
}

// chooseDownloadNode 选择一个下载节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (svc *ObjectService) chooseDownloadNode(entries []coormsg.ReadRespEntry) coormsg.ReadRespEntry {
	sameLocationEntries := lo.Filter(entries, func(e coormsg.ReadRespEntry, i int) bool { return e.IsSameLocation })
	if len(sameLocationEntries) > 0 {
		return sameLocationEntries[rand.Int()%len(sameLocationEntries)]
	}

	return entries[rand.Int()%len(entries)]
}

func (svc *ObjectService) downloadAsRepObject(nodeIP string, fileHash string) (io.ReadCloser, error) {
	// 连接grpc
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}

	/*
		TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
			如果本地有ipfs daemon且能获取相应对象的cid，则获取对象cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
			否则，像目前一样，使用grpc向指定节点获取
	*/
	// 下载文件
	client := agentcaller.NewFileTransportClient(conn)
	reader, err := mygrpc.GetFileAsStream(client, fileHash)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("request to get file failed, err: %w", err)
	}

	reader = myio.AfterReadClosed(reader, func(io.ReadCloser) { conn.Close() })
	return reader, nil
}

func (svc *ObjectService) UploadRepObject(userID int, bucketID int, objectName string, file io.ReadCloser, fileSize int64, repNum int) error {

	//发送写请求，请求Coor分配写入节点Ip
	repWriteResp, err := svc.coordinator.RepWrite(coormsg.NewRepWriteCommandBody(bucketID, objectName, fileSize, repNum, userID, config.Cfg().ExternalIP))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if repWriteResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator RepWrite failed, code: %s, message: %s", repWriteResp.ErrorCode, repWriteResp.ErrorMessage)
	}

	/*
		TO DO ss: 判断本地有没有ipfs daemon、能否与目标agent的ipfs daemon连通、本地ipfs目录空间是否充足
			如果本地有ipfs daemon、能与目标agent的ipfs daemon连通、本地ipfs目录空间充足，将所有内容写入本地ipfs目录，得到对象的cid，发送cid给目标agent让其pin相应的对象
			否则，像目前一样，使用grpc向指定节点获取
	*/

	senders := make([]fileSender, repNum)

	// 建立grpc连接，发送请求
	svc.startSendFile(repNum, senders, repWriteResp.Body.Nodes)

	// 向每个节点发送数据
	err = svc.sendFileData(file, repNum, senders)
	if err != nil {
		return err
	}

	// 发送EOF消息，并获得FileHash
	svc.sendFinish(repNum, senders)

	// 收集发送成功的节点以及返回的hash
	var sucNodeIDs []int
	var sucFileHashes []string
	for i := 0; i < repNum; i++ {
		sender := &senders[i]

		if sender.err == nil {
			sucNodeIDs = append(sucNodeIDs, sender.nodeID)
			sucFileHashes = append(sucFileHashes, sender.fileHash)
		}
	}

	// 记录写入的文件的Hash
	// TODO 如果一个都没有写成功，那么是否要发送这个请求？
	writeRepHashResp, err := svc.coordinator.WriteRepHash(coormsg.NewWriteRepHashCommandBody(bucketID, objectName, fileSize, repNum, userID, sucNodeIDs, sucFileHashes))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if writeRepHashResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator WriteRepHash failed, code: %s, message: %s", writeRepHashResp.ErrorCode, writeRepHashResp.ErrorMessage)
	}

	return nil
}

type fileSender struct {
	grpcCon  *grpc.ClientConn
	stream   mygrpc.FileWriteCloser[string]
	nodeID   int
	fileHash string
	err      error
}

func (svc *ObjectService) startSendFile(numRep int, senders []fileSender, nodes []coormsg.WriteRespNode) {
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		sender.nodeID = nodes[i].ID

		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := nodes[i].ExternalIP
		if nodes[i].IsSameLocation {
			nodeIP = nodes[i].LocalIP
			// TODO 以后考虑用log
			fmt.Printf("client and node %d are at the same location, use local ip\n", nodes[i].ID)
		}

		grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
		conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			sender.err = fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
			continue
		}

		client := agentcaller.NewFileTransportClient(conn)
		stream, err := mygrpc.SendFileAsStream(client)
		if err != nil {
			conn.Close()
			sender.err = fmt.Errorf("request to send file failed, err: %w", err)
			continue
		}

		sender.grpcCon = conn
		sender.stream = stream
	}
}

func (svc *ObjectService) sendFileData(file io.ReadCloser, numRep int, senders []fileSender) error {

	// 共用的发送数据缓冲区
	buf := make([]byte, 2048)

	for {
		// 读取文件数据
		readCnt, err := file.Read(buf)

		if readCnt > 0 {
			// 并行的向每个节点发送数据
			hasSender := false
			var sendWg sync.WaitGroup
			for i := 0; i < numRep; i++ {
				sender := &senders[i]

				// 发生了错误的跳过
				if sender.err != nil {
					continue
				}

				hasSender = true

				sendWg.Add(1)
				go func() {
					err := myio.WriteAll(sender.stream, buf[:readCnt])

					// 发生错误则关闭连接
					if err != nil {
						sender.stream.Abort(io.ErrClosedPipe)
						sender.grpcCon.Close()
						sender.err = fmt.Errorf("send file data failed, err: %w", err)
					}

					sendWg.Done()
				}()
			}

			// 等待向每个节点发送数据结束
			sendWg.Wait()

			// 如果所有节点都发送失败，则不要再继续读取文件数据了
			if !hasSender {
				break
			}
		}

		// 文件读取完毕
		if err == io.EOF {
			break
		}

		if err != nil {
			// 读取失败则断开所有连接
			for i := 0; i < numRep; i++ {
				sender := &senders[i]

				if sender.err != nil {
					continue
				}

				sender.stream.Abort(io.ErrClosedPipe)
				sender.grpcCon.Close()
				sender.err = fmt.Errorf("read file data failed, err: %w", err)
			}

			return fmt.Errorf("read file data failed, err: %w", err)
		}
	}
	return nil
}

func (svc *ObjectService) sendFinish(numRep int, senders []fileSender) {
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		// 发生了错误的跳过
		if sender.err != nil {
			continue
		}

		fileHash, err := sender.stream.Finish()
		if err != nil {
			sender.err = err
			sender.stream.Abort(io.ErrClosedPipe)
			sender.grpcCon.Close()
			continue
		}

		sender.fileHash = fileHash
		sender.err = err
		sender.grpcCon.Close()
	}
}

func (svc *ObjectService) UploadECObject(userID int, file io.ReadCloser, fileSize int64, ecName string) error {
	// TODO
	panic("not implement yet")
}

func (svc *ObjectService) DeleteObject(userID int, objectID int) error {
	resp, err := svc.coordinator.DeleteObject(coormsg.NewDeleteObjectBody(userID, objectID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if !resp.IsOK() {
		return fmt.Errorf("create bucket objects failed, code: %s, message: %s", resp.ErrorCode, resp.ErrorMessage)
	}

	return nil
}
