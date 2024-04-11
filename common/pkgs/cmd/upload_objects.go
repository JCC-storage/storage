package cmd

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// UploadObjects 上传对象的结构体，包含上传所需的用户ID、包ID、对象迭代器和节点亲和性信息。
type UploadObjects struct {
	userID       cdssdk.UserID
	packageID    cdssdk.PackageID
	objectIter   iterator.UploadingObjectIterator
	nodeAffinity *cdssdk.NodeID
}

// UploadObjectsResult 上传对象结果的结构体，包含上传结果的数组。
type UploadObjectsResult struct {
	Objects []ObjectUploadResult
}

// ObjectUploadResult 单个对象上传结果的结构体，包含上传信息、错误和对象ID。
type ObjectUploadResult struct {
	Info  *iterator.IterUploadingObject
	Error error
	// TODO 这个字段没有被赋值
	ObjectID cdssdk.ObjectID
}

// UploadNodeInfo 上传节点信息的结构体，包含节点信息、延迟、是否与客户端在同一位置。
type UploadNodeInfo struct {
	Node           cdssdk.Node
	Delay          time.Duration
	IsSameLocation bool
}

// UploadObjectsContext 上传对象上下文的结构体，包含分布式锁服务和连通性收集器。
type UploadObjectsContext struct {
	Distlock     *distlock.Service
	Connectivity *connectivity.Collector
}

// NewUploadObjects 创建一个新的UploadObjects实例。
func NewUploadObjects(userID cdssdk.UserID, packageID cdssdk.PackageID, objIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) *UploadObjects {
	return &UploadObjects{
		userID:       userID,
		packageID:    packageID,
		objectIter:   objIter,
		nodeAffinity: nodeAffinity,
	}
}

// Execute 执行上传对象的操作。
func (t *UploadObjects) Execute(ctx *UploadObjectsContext) (*UploadObjectsResult, error) {
	defer t.objectIter.Close()

	// 获取协调器客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	// 获取用户节点信息
	getUserNodesResp, err := coorCli.GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return nil, fmt.Errorf("getting user nodes: %w", err)
	}

	// 获取节点连通性信息
	cons := ctx.Connectivity.GetAll()
	userNodes := lo.Map(getUserNodesResp.Nodes, func(node cdssdk.Node, index int) UploadNodeInfo {
		delay := time.Duration(math.MaxInt64)

		con, ok := cons[node.NodeID]
		if ok && con.Delay != nil {
			delay = *con.Delay
		}

		return UploadNodeInfo{
			Node:           node,
			Delay:          delay,
			IsSameLocation: node.LocationID == stgglb.Local.LocationID,
		}
	})
	if len(userNodes) == 0 {
		return nil, fmt.Errorf("user no available nodes")
	}

	// 对上传节点的IPFS加锁
	ipfsReqBlder := reqbuilder.NewBuilder()
	if stgglb.Local.NodeID != nil {
		ipfsReqBlder.IPFS().Buzy(*stgglb.Local.NodeID)
	}
	for _, node := range userNodes {
		if stgglb.Local.NodeID != nil && node.Node.NodeID == *stgglb.Local.NodeID {
			continue
		}

		ipfsReqBlder.IPFS().Buzy(node.Node.NodeID)
	}

	// 获得IPFS锁
	ipfsMutex, err := ipfsReqBlder.MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer ipfsMutex.Unlock()

	// 上传并更新包信息
	rets, err := uploadAndUpdatePackage(t.packageID, t.objectIter, userNodes, t.nodeAffinity)
	if err != nil {
		return nil, err
	}

	return &UploadObjectsResult{
		Objects: rets,
	}, nil
}

// chooseUploadNode 选择一个上传文件的节点。
// 首先选择设置了亲和性的节点，然后从与当前客户端相同地域的节点中随机选择一个，最后选择延迟最低的节点。
func chooseUploadNode(nodes []UploadNodeInfo, nodeAffinity *cdssdk.NodeID) UploadNodeInfo {
	if nodeAffinity != nil {
		aff, ok := lo.Find(nodes, func(node UploadNodeInfo) bool { return node.Node.NodeID == *nodeAffinity })
		if ok {
			return aff
		}
	}

	sameLocationNodes := lo.Filter(nodes, func(e UploadNodeInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationNodes) > 0 {
		return sameLocationNodes[rand.Intn(len(sameLocationNodes))]
	}

	// 选择延迟最低的节点
	nodes = sort2.Sort(nodes, func(e1, e2 UploadNodeInfo) int { return sort2.Cmp(e1.Delay, e2.Delay) })

	return nodes[0]
}

// uploadAndUpdatePackage 上传文件并更新包信息。
// packageID：标识待更新的包的ID。
// objectIter：提供上传对象迭代器，用于遍历上传的文件。
// userNodes：用户可选的上传节点信息列表。
// nodeAffinity：用户首选的上传节点。
// 返回值：上传结果列表和错误信息。
func uploadAndUpdatePackage(packageID cdssdk.PackageID, objectIter iterator.UploadingObjectIterator, userNodes []UploadNodeInfo, nodeAffinity *cdssdk.NodeID) ([]ObjectUploadResult, error) {
	// 获取协调器客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 选择上传节点
	uploadNode := chooseUploadNode(userNodes, nodeAffinity)

	var uploadRets []ObjectUploadResult
	// 构建添加对象的列表
	var adds []coormq.AddObjectEntry
	for {
		// 获取下一个对象信息。如果不存在更多对象，则退出循环。
		objInfo, err := objectIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			// 对象获取发生错误，返回错误信息。
			return nil, fmt.Errorf("reading object: %w", err)
		}

		// 执行上传逻辑，每个对象依次执行。
		err = func() error {
			// 确保对象文件在函数退出时关闭。
			defer objInfo.File.Close()

			// 记录上传开始时间。
			uploadTime := time.Now()
			// 上传文件，并获取文件哈希值。
			fileHash, err := uploadFile(objInfo.File, uploadNode)
			if err != nil {
				// 文件上传失败，记录错误信息并返回。
				return fmt.Errorf("uploading file: %w", err)
			}

			// 收集上传结果。
			uploadRets = append(uploadRets, ObjectUploadResult{
				Info:  objInfo,
				Error: err,
			})

			// 准备添加到队列的条目，以供后续处理。
			adds = append(adds, coormq.NewAddObjectEntry(objInfo.Path, objInfo.Size, fileHash, uploadTime, uploadNode.Node.NodeID))
			return nil
		}()
		if err != nil {
			// 上传操作中出现错误，返回错误信息。
			return nil, err
		}
	}

	// 更新包信息
	_, err = coorCli.UpdatePackage(coormq.NewUpdatePackage(packageID, adds, nil))
	if err != nil {
		return nil, fmt.Errorf("updating package: %w", err)
	}

	return uploadRets, nil
}

// uploadFile 上传文件。
// file：待上传的文件流。
// uploadNode：指定的上传节点信息。
// 返回值：文件哈希和错误信息。
func uploadFile(file io.Reader, uploadNode UploadNodeInfo) (string, error) {
	// 尝试使用本地IPFS上传
	if stgglb.IPFSPool != nil {
		logger.Infof("try to use local IPFS to upload file")

		fileHash, err := uploadToLocalIPFS(file, uploadNode.Node.NodeID, stgglb.Local.NodeID == nil)
		if err == nil {
			return fileHash, nil
		} else {
			logger.Warnf("upload to local IPFS failed, so try to upload to node %d, err: %s", uploadNode.Node.NodeID, err.Error())
		}
	}

	// 否则，发送到agent进行上传
	nodeIP := uploadNode.Node.ExternalIP
	grpcPort := uploadNode.Node.ExternalGRPCPort
	if uploadNode.IsSameLocation {
		nodeIP = uploadNode.Node.LocalIP
		grpcPort = uploadNode.Node.LocalGRPCPort
		logger.Infof("client and node %d are at the same location, use local ip", uploadNode.Node.NodeID)
	}

	fileHash, err := uploadToNode(file, nodeIP, grpcPort)
	if err != nil {
		return "", fmt.Errorf("upload to node %s failed, err: %w", nodeIP, err)
	}

	return fileHash, nil
}

// uploadToNode 发送文件到指定的节点。
// file：文件流。
// nodeIP：节点的IP地址。
// grpcPort：节点的gRPC端口。
// 返回值：文件哈希和错误信息。
func uploadToNode(file io.Reader, nodeIP string, grpcPort int) (string, error) {
	rpcCli, err := stgglb.AgentRPCPool.Acquire(nodeIP, grpcPort)
	if err != nil {
		return "", fmt.Errorf("new agent rpc client: %w", err)
	}
	defer rpcCli.Close()

	return rpcCli.SendIPFSFile(file)
}

// uploadToLocalIPFS 将文件上传到本地的IPFS节点，并根据需要将文件固定（pin）在节点上。
// file: 要上传的文件，作为io.Reader提供。
// nodeID: 指定上传到的IPFS节点的ID。
// shouldPin: 指示是否在IPFS节点上固定（pin）上传的文件。如果为true，则文件会被固定，否则不会。
// 返回上传文件的IPFS哈希值和可能出现的错误。
func uploadToLocalIPFS(file io.Reader, nodeID cdssdk.NodeID, shouldPin bool) (string, error) {
	// 从IPFS池获取一个IPFS客户端实例
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return "", fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close() // 确保IPFS客户端在函数返回前被释放

	// 在IPFS上创建文件并获取其哈希值
	fileHash, err := ipfsCli.CreateFile(file)
	if err != nil {
		return "", fmt.Errorf("creating ipfs file: %w", err)
	}

	// 如果不需要固定文件，则直接返回文件哈希值
	if !shouldPin {
		return fileHash, nil
	}

	// 将文件固定在IPFS节点上
	err = pinIPFSFile(nodeID, fileHash)
	if err != nil {
		return "", err
	}

	return fileHash, nil
}

// pinIPFSFile 将文件Pin到IPFS节点。
// nodeID：节点ID。
// fileHash：文件哈希。
// 返回值：错误信息。
func pinIPFSFile(nodeID cdssdk.NodeID, fileHash string) error {
	agtCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	_, err = agtCli.PinObject(agtmq.ReqPinObject([]string{fileHash}, false))
	if err != nil {
		return fmt.Errorf("start pinning object: %w", err)
	}

	return nil
}
