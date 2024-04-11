package task

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	myref "gitlink.org.cn/cloudream/common/utils/reflect"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/utils"
)

// StorageLoadPackage 定义了存储加载包的结构体，包含完整的输出路径和与存储、包、用户相关的ID。
type StorageLoadPackage struct {
	FullOutputPath string

	userID       cdssdk.UserID
	packageID    cdssdk.PackageID
	storageID    cdssdk.StorageID
	pinnedBlocks []stgmod.ObjectBlock
}

// NewStorageLoadPackage 创建一个新的StorageLoadPackage实例。
// userID: 用户ID。
// packageID: 包ID。
// storageID: 存储ID。
// 返回一个新的StorageLoadPackage指针。
func NewStorageLoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID) *StorageLoadPackage {
	return &StorageLoadPackage{
		userID:    userID,
		packageID: packageID,
		storageID: storageID,
	}
}

// Execute 执行存储加载任务。
// task: 任务实例。
// ctx: 任务上下文。
// complete: 完成回调函数。
// 无返回值。
func (t *StorageLoadPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	err := t.do(task, ctx)

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

// do 实际执行存储加载的过程。
// task: 任务实例。
// ctx: 任务上下文。
// 返回执行过程中可能出现的错误。
func (t *StorageLoadPackage) do(task *task.Task[TaskContext], ctx TaskContext) error {
	// 获取协调器客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 获取IPFS客户端
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new IPFS client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	// 从协调器获取存储信息
	getStgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(t.userID, t.storageID))
	if err != nil {
		return fmt.Errorf("request to coordinator: %w", err)
	}

	// 构造输出目录路径并创建该目录
	outputDirPath := utils.MakeStorageLoadPackagePath(getStgResp.Directory, t.userID, t.packageID)
	if err = os.MkdirAll(outputDirPath, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}
	t.FullOutputPath = outputDirPath

	// 获取包对象详情
	getObjectDetails, err := coorCli.GetPackageObjectDetails(coormq.NewGetPackageObjectDetails(t.packageID))
	if err != nil {
		return fmt.Errorf("getting package object details: %w", err)
	}

	// 获取互斥锁以确保并发安全
	mutex, err := reqbuilder.NewBuilder().
		// 提前占位
		Metadata().StoragePackage().CreateOne(t.userID, t.storageID, t.packageID).
		// 保护在storage目录中下载的文件
		Storage().Buzy(t.storageID).
		// 保护下载文件时同时保存到IPFS的文件
		IPFS().Buzy(getStgResp.NodeID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	// 下载每个对象
	for _, obj := range getObjectDetails.Objects {
		err := t.downloadOne(coorCli, ipfsCli, outputDirPath, obj)
		if err != nil {
			return err
		}
	}

	// 通知协调器包已加载到存储
	_, err = coorCli.StoragePackageLoaded(coormq.NewStoragePackageLoaded(t.userID, t.storageID, t.packageID, t.pinnedBlocks))
	if err != nil {
		return fmt.Errorf("loading package to storage: %w", err)
	}

	// TODO 要防止下载的临时文件被删除
	return err
}

// downloadOne 用于下载一种特定冗余类型的对象。
//
// 参数:
// - coorCli: 协调客户端，用于与CDN协调器进行通信。
// - ipfsCli: IPFS池客户端，用于与IPFS网络进行交互。
// - dir: 下载对象的目标目录。
// - obj: 要下载的对象详细信息，包括对象路径和冗余类型等。
//
// 返回值:
// - error: 下载过程中遇到的任何错误。
func (t *StorageLoadPackage) downloadOne(coorCli *coormq.Client, ipfsCli *ipfs.PoolClient, dir string, obj stgmod.ObjectDetail) error {
	var file io.ReadCloser

	// 根据对象的冗余类型选择不同的下载策略。
	switch red := obj.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		// 无冗余或复制冗余对象的下载处理。
		reader, err := t.downloadNoneOrRepObject(ipfsCli, obj)
		if err != nil {
			return fmt.Errorf("downloading object: %w", err)
		}
		file = reader

	case *cdssdk.RepRedundancy:
		// 复制冗余对象的下载处理。
		reader, err := t.downloadNoneOrRepObject(ipfsCli, obj)
		if err != nil {
			return fmt.Errorf("downloading rep object: %w", err)
		}
		file = reader

	case *cdssdk.ECRedundancy:
		// 前向纠错冗余对象的下载处理。
		reader, pinnedBlocks, err := t.downloadECObject(coorCli, ipfsCli, obj, red)
		if err != nil {
			return fmt.Errorf("downloading ec object: %w", err)
		}
		file = reader
		t.pinnedBlocks = append(t.pinnedBlocks, pinnedBlocks...)

	default:
		// 遇到未知的冗余类型返回错误。
		return fmt.Errorf("unknow redundancy type: %v", myref.TypeOfValue(obj.Object.Redundancy))
	}
	defer file.Close() // 确保文件在函数返回前被关闭。

	// 拼接完整的文件路径，并创建包含该文件的目录。
	fullPath := filepath.Join(dir, obj.Object.Path)

	lastDirPath := filepath.Dir(fullPath)
	if err := os.MkdirAll(lastDirPath, 0755); err != nil {
		return fmt.Errorf("creating object last dir: %w", err)
	}

	// 创建输出文件。
	outputFile, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("creating object file: %w", err)
	}
	defer outputFile.Close() // 确保文件在函数返回前被关闭。

	// 将下载的内容写入本地文件。
	if _, err := io.Copy(outputFile, file); err != nil {
		return fmt.Errorf("writting object to file: %w", err)
	}

	return nil
}

// downloadNoneOrRepObject 用于下载没有冗余或需要从IPFS网络中检索的对象。
// 如果对象不存在于任何节点上，则返回错误。
//
// 参数:
// - ipfsCli: IPFS客户端池的指针，用于与IPFS网络交互。
// - obj: 要下载的对象的详细信息。
//
// 返回值:
// - io.ReadCloser: 下载文件的读取器。
// - error: 如果下载过程中出现错误，则返回错误信息。
func (t *StorageLoadPackage) downloadNoneOrRepObject(ipfsCli *ipfs.PoolClient, obj stgmod.ObjectDetail) (io.ReadCloser, error) {
	if len(obj.Blocks) == 0 && len(obj.PinnedAt) == 0 {
		return nil, fmt.Errorf("no node has this object")
	}

	// 将对象文件哈希添加到本地Pin列表，无论是否真正需要
	ipfsCli.Pin(obj.Object.FileHash)

	// 尝试打开并读取对象文件
	file, err := ipfsCli.OpenRead(obj.Object.FileHash)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// downloadECObject 用于下载采用EC（Erasure Coding）编码的对象。
// 该方法会根据对象的块信息和EC冗余策略，从网络中下载必要的数据块并恢复整个对象。
//
// 参数:
// - coorCli: 协调器客户端的指针，用于节点间的协调与通信。
// - ipfsCli: IPFS客户端池的指针，用于与IPFS网络交互。
// - obj: 要下载的对象的详细信息。
// - ecRed: EC冗余策略的详细配置。
//
// 返回值:
// - io.ReadCloser: 恢复后的对象文件的读取器。
// - []stgmod.ObjectBlock: 被Pin住的对象块列表。
// - error: 如果下载或恢复过程中出现错误，则返回错误信息。
func (t *StorageLoadPackage) downloadECObject(coorCli *coormq.Client, ipfsCli *ipfs.PoolClient, obj stgmod.ObjectDetail, ecRed *cdssdk.ECRedundancy) (io.ReadCloser, []stgmod.ObjectBlock, error) {
	// 根据对象信息和节点状态，排序选择最优的下载节点
	allNodes, err := t.sortDownloadNodes(coorCli, obj)
	if err != nil {
		return nil, nil, err
	}

	// 计算最小读取块解决方案和最小读取对象解决方案
	bsc, blocks := t.getMinReadingBlockSolution(allNodes, ecRed.K)
	osc, _ := t.getMinReadingObjectSolution(allNodes, ecRed.K)

	// 如果通过块恢复更高效，则执行块恢复流程
	if bsc < osc {
		var fileStrs []io.ReadCloser

		// 初始化RS编码器
		rs, err := ec.NewRs(ecRed.K, ecRed.N, ecRed.ChunkSize)
		if err != nil {
			return nil, nil, fmt.Errorf("new rs: %w", err)
		}

		// 为每个需要读取的块执行Pin操作和打开读取流
		for i := range blocks {
			ipfsCli.Pin(blocks[i].Block.FileHash)

			str, err := ipfsCli.OpenRead(blocks[i].Block.FileHash)
			if err != nil {
				for i -= 1; i >= 0; i-- {
					fileStrs[i].Close()
				}
				return nil, nil, fmt.Errorf("donwloading file: %w", err)
			}

			fileStrs = append(fileStrs, str)
		}

		// 将多个文件流转换为统一的ReadCloser接口
		fileReaders, filesCloser := myio.ToReaders(fileStrs)

		// 准备恢复数据所需的信息和变量
		var indexes []int
		var pinnedBlocks []stgmod.ObjectBlock
		for _, b := range blocks {
			indexes = append(indexes, b.Block.Index)
			pinnedBlocks = append(pinnedBlocks, stgmod.ObjectBlock{
				ObjectID: b.Block.ObjectID,
				Index:    b.Block.Index,
				NodeID:   *stgglb.Local.NodeID,
				FileHash: b.Block.FileHash,
			})
		}

		// 执行数据恢复，并将恢复后的数据转换为ReadCloser
		outputs, outputsCloser := myio.ToReaders(rs.ReconstructData(fileReaders, indexes))
		return myio.AfterReadClosed(myio.Length(myio.ChunkedJoin(outputs, int(ecRed.ChunkSize)), obj.Object.Size), func(c io.ReadCloser) {
			filesCloser()
			outputsCloser()
		}), pinnedBlocks, nil
	}

	// 如果通过对象恢复更高效或没有足够的块来恢复文件，则直接尝试读取对象文件
	if osc == math.MaxFloat64 {
		return nil, nil, fmt.Errorf("no enough blocks to reconstruct the file, want %d, get only %d", ecRed.K, len(blocks))
	}

	str, err := ipfsCli.OpenRead(obj.Object.FileHash)
	return str, nil, err
}

type downloadNodeInfo struct {
	Node         cdssdk.Node
	ObjectPinned bool
	Blocks       []stgmod.ObjectBlock
	Distance     float64
}

// sortDownloadNodes 对存储对象的下载节点进行排序
// 参数:
// - coorCli *coormq.Client: 协调器客户端，用于获取节点信息
// - obj stgmod.ObjectDetail: 存储对象的详细信息，包含固定存储节点和数据块信息
// 返回值:
// - []*downloadNodeInfo: 排序后的下载节点信息数组
// - error: 如果过程中发生错误，则返回错误信息
func (t *StorageLoadPackage) sortDownloadNodes(coorCli *coormq.Client, obj stgmod.ObjectDetail) ([]*downloadNodeInfo, error) {
	// 收集对象的固定存储节点ID和数据块所在节点ID
	var nodeIDs []cdssdk.NodeID
	for _, id := range obj.PinnedAt {
		if !lo.Contains(nodeIDs, id) {
			nodeIDs = append(nodeIDs, id)
		}
	}
	for _, b := range obj.Blocks {
		if !lo.Contains(nodeIDs, b.NodeID) {
			nodeIDs = append(nodeIDs, b.NodeID)
		}
	}

	// 获取节点信息
	getNodes, err := coorCli.GetNodes(coormq.NewGetNodes(nodeIDs))
	if err != nil {
		return nil, fmt.Errorf("getting nodes: %w", err)
	}

	// 建立下载节点信息的映射表
	downloadNodeMap := make(map[cdssdk.NodeID]*downloadNodeInfo)
	for _, id := range obj.PinnedAt {
		node, ok := downloadNodeMap[id]
		if !ok {
			mod := *getNodes.GetNode(id)
			node = &downloadNodeInfo{
				Node:         mod,
				ObjectPinned: true,
				Distance:     t.getNodeDistance(mod),
			}
			downloadNodeMap[id] = node
		}

		node.ObjectPinned = true // 标记为固定存储对象
	}

	// 为每个数据块所在节点填充信息，并收集到映射表中
	for _, b := range obj.Blocks {
		node, ok := downloadNodeMap[b.NodeID]
		if !ok {
			mod := *getNodes.GetNode(b.NodeID)
			node = &downloadNodeInfo{
				Node:     mod,
				Distance: t.getNodeDistance(mod),
			}
			downloadNodeMap[b.NodeID] = node
		}

		node.Blocks = append(node.Blocks, b) // 添加数据块信息
	}

	// 根据节点与存储对象的距离进行排序
	return sort2.Sort(lo.Values(downloadNodeMap), func(left, right *downloadNodeInfo) int {
		return sort2.Cmp(left.Distance, right.Distance)
	}), nil
}

type downloadBlock struct {
	Node  cdssdk.Node
	Block stgmod.ObjectBlock
}

// getMinReadingBlockSolution 获取最小读取区块解决方案
// sortedNodes: 已排序的节点信息列表，每个节点包含多个区块信息
// k: 需要获取的区块数量
// 返回值: 返回获取到的区块的总距离和区块列表
func (t *StorageLoadPackage) getMinReadingBlockSolution(sortedNodes []*downloadNodeInfo, k int) (float64, []downloadBlock) {
	// 初始化已获取区块的bitmap和距离
	gotBlocksMap := bitmap.Bitmap64(0)
	var gotBlocks []downloadBlock
	dist := float64(0.0)
	// 遍历所有节点及其区块，直到获取到k个不同的区块
	for _, n := range sortedNodes {
		for _, b := range n.Blocks {
			// 如果区块未被获取，则添加到列表中，并更新距离
			if !gotBlocksMap.Get(b.Index) {
				gotBlocks = append(gotBlocks, downloadBlock{
					Node:  n.Node,
					Block: b,
				})
				gotBlocksMap.Set(b.Index, true)
				dist += n.Distance
			}

			// 如果已获取的区块数量达到k，返回结果
			if len(gotBlocks) >= k {
				return dist, gotBlocks
			}
		}
	}

	// 如果无法获取到k个不同的区块，返回最大距离和空的区块列表
	return math.MaxFloat64, gotBlocks
}

// getMinReadingObjectSolution 获取最小读取对象解决方案
// sortedNodes: 已排序的节点信息列表，每个节点包含一个对象是否被固定的信息
// k: 需要获取的对象数量
// 返回值: 返回获取对象的最小距离和对应的节点
func (t *StorageLoadPackage) getMinReadingObjectSolution(sortedNodes []*downloadNodeInfo, k int) (float64, *cdssdk.Node) {
	dist := math.MaxFloat64
	var downloadNode *cdssdk.Node
	// 遍历节点，寻找距离最小且对象被固定的节点
	for _, n := range sortedNodes {
		if n.ObjectPinned && float64(k)*n.Distance < dist {
			dist = float64(k) * n.Distance
			downloadNode = &n.Node
		}
	}

	return dist, downloadNode
}

// getNodeDistance 获取节点距离
// node: 需要计算距离的节点
// 返回值: 返回节点与当前节点或位置的距离
func (t *StorageLoadPackage) getNodeDistance(node cdssdk.Node) float64 {
	// 如果有本地节点ID且与目标节点ID相同，返回同一节点距离
	if stgglb.Local.NodeID != nil {
		if node.NodeID == *stgglb.Local.NodeID {
			return consts.NodeDistanceSameNode
		}
	}

	// 如果节点位置与本地位置相同，返回同一位置距离
	if node.LocationID == stgglb.Local.LocationID {
		return consts.NodeDistanceSameLocation
	}

	// 默认返回其他距离
	return consts.NodeDistanceOther
}
