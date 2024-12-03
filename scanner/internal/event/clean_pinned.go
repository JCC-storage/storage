package event

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type CleanPinned struct {
	*scevt.CleanPinned
}

func NewCleanPinned(evt *scevt.CleanPinned) *CleanPinned {
	return &CleanPinned{
		CleanPinned: evt,
	}
}

func (t *CleanPinned) TryMerge(other Event) bool {
	event, ok := other.(*CleanPinned)
	if !ok {
		return false
	}

	return t.PackageID == event.PackageID
}

func (t *CleanPinned) Execute(execCtx ExecuteContext) {
	log := logger.WithType[CleanPinned]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.CleanPinned))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	// TODO 应该与其他event一样，直接访问数据库
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		log.Warnf("new coordinator client: %s", err.Error())
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getObjs, err := coorCli.GetPackageObjectDetails(coormq.ReqGetPackageObjectDetails(t.PackageID))
	if err != nil {
		log.Warnf("getting package objects: %s", err.Error())
		return
	}

	stats, err := execCtx.Args.DB.PackageAccessStat().GetByPackageID(execCtx.Args.DB.DefCtx(), t.PackageID)
	if err != nil {
		log.Warnf("getting package access stat: %s", err.Error())
		return
	}
	var readerStgIDs []cdssdk.StorageID
	for _, item := range stats {
		// TODO 可以考虑做成配置
		if item.Amount >= float64(len(getObjs.Objects)/2) {
			readerStgIDs = append(readerStgIDs, item.StorageID)
		}
	}

	// 注意！需要保证allStgID包含所有之后可能用到的节点ID
	// TOOD 可以考虑设计Cache机制
	var allStgID []cdssdk.StorageID
	for _, obj := range getObjs.Objects {
		for _, block := range obj.Blocks {
			allStgID = append(allStgID, block.StorageID)
		}
		allStgID = append(allStgID, obj.PinnedAt...)
	}
	allStgID = append(allStgID, readerStgIDs...)

	getStgResp, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails(lo.Union(allStgID)))
	if err != nil {
		log.Warnf("getting nodes: %s", err.Error())
		return
	}

	allStgInfos := make(map[cdssdk.StorageID]*stgmod.StorageDetail)
	for _, stg := range getStgResp.Storages {
		allStgInfos[stg.Storage.StorageID] = stg
	}

	// 只对ec和rep对象进行处理
	var ecObjects []stgmod.ObjectDetail
	var repObjects []stgmod.ObjectDetail
	for _, obj := range getObjs.Objects {
		if _, ok := obj.Object.Redundancy.(*cdssdk.ECRedundancy); ok {
			ecObjects = append(ecObjects, obj)
		} else if _, ok := obj.Object.Redundancy.(*cdssdk.RepRedundancy); ok {
			repObjects = append(repObjects, obj)
		}
	}

	planBld := exec.NewPlanBuilder()
	planningStgIDs := make(map[cdssdk.StorageID]bool)

	// 对于rep对象，统计出所有对象块分布最多的两个节点，用这两个节点代表所有rep对象块的分布，去进行退火算法
	var repObjectsUpdating []coormq.UpdatingObjectRedundancy
	repMostHubIDs := t.summaryRepObjectBlockNodes(repObjects)
	solu := t.startAnnealing(allStgInfos, readerStgIDs, annealingObject{
		totalBlockCount: 1,
		minBlockCnt:     1,
		pinnedAt:        repMostHubIDs,
		blocks:          nil,
	})
	for _, obj := range repObjects {
		repObjectsUpdating = append(repObjectsUpdating, t.makePlansForRepObject(allStgInfos, solu, obj, planBld, planningStgIDs))
	}

	// 对于ec对象，则每个对象单独进行退火算法
	var ecObjectsUpdating []coormq.UpdatingObjectRedundancy
	for _, obj := range ecObjects {
		ecRed := obj.Object.Redundancy.(*cdssdk.ECRedundancy)
		solu := t.startAnnealing(allStgInfos, readerStgIDs, annealingObject{
			totalBlockCount: ecRed.N,
			minBlockCnt:     ecRed.K,
			pinnedAt:        obj.PinnedAt,
			blocks:          obj.Blocks,
		})
		ecObjectsUpdating = append(ecObjectsUpdating, t.makePlansForECObject(allStgInfos, solu, obj, planBld, planningStgIDs))
	}

	ioSwRets, err := t.executePlans(execCtx, planBld, planningStgIDs)
	if err != nil {
		log.Warn(err.Error())
		return
	}

	// 根据按照方案进行调整的结果，填充更新元数据的命令
	for i := range ecObjectsUpdating {
		t.populateECObjectEntry(&ecObjectsUpdating[i], ecObjects[i], ioSwRets)
	}

	finalEntries := append(repObjectsUpdating, ecObjectsUpdating...)
	if len(finalEntries) > 0 {
		_, err = coorCli.UpdateObjectRedundancy(coormq.ReqUpdateObjectRedundancy(finalEntries))
		if err != nil {
			log.Warnf("changing object redundancy: %s", err.Error())
			return
		}
	}
}

func (t *CleanPinned) summaryRepObjectBlockNodes(objs []stgmod.ObjectDetail) []cdssdk.StorageID {
	type stgBlocks struct {
		StorageID cdssdk.StorageID
		Count     int
	}

	stgBlocksMap := make(map[cdssdk.StorageID]*stgBlocks)
	for _, obj := range objs {
		cacheBlockStgs := make(map[cdssdk.StorageID]bool)
		for _, block := range obj.Blocks {
			if _, ok := stgBlocksMap[block.StorageID]; !ok {
				stgBlocksMap[block.StorageID] = &stgBlocks{
					StorageID: block.StorageID,
					Count:     0,
				}
			}
			stgBlocksMap[block.StorageID].Count++
			cacheBlockStgs[block.StorageID] = true
		}

		for _, hubID := range obj.PinnedAt {
			if cacheBlockStgs[hubID] {
				continue
			}

			if _, ok := stgBlocksMap[hubID]; !ok {
				stgBlocksMap[hubID] = &stgBlocks{
					StorageID: hubID,
					Count:     0,
				}
			}
			stgBlocksMap[hubID].Count++
		}
	}

	stgs := lo.Values(stgBlocksMap)
	sort2.Sort(stgs, func(left *stgBlocks, right *stgBlocks) int {
		return right.Count - left.Count
	})

	// 只选出块数超过一半的节点，但要保证至少有两个节点
	for i := 2; i < len(stgs); i++ {
		if stgs[i].Count < len(objs)/2 {
			stgs = stgs[:i]
			break
		}
	}

	return lo.Map(stgs, func(item *stgBlocks, idx int) cdssdk.StorageID { return item.StorageID })
}

type annealingState struct {
	allStgInfos        map[cdssdk.StorageID]*stgmod.StorageDetail // 所有节点的信息
	readerStgIDs       []cdssdk.StorageID                         // 近期可能访问此对象的节点
	stgsSortedByReader map[cdssdk.StorageID][]stgDist             // 拥有数据的节点到每个可能访问对象的节点按距离排序
	object             annealingObject                            // 进行退火的对象
	blockList          []objectBlock                              // 排序后的块分布情况
	stgBlockBitmaps    map[cdssdk.StorageID]*bitmap.Bitmap64      // 用位图的形式表示每一个节点上有哪些块
	stgCombTree        combinatorialTree                          // 节点组合树，用于加速计算容灾度

	maxScore         float64 // 搜索过程中得到过的最大分数
	maxScoreRmBlocks []bool  // 最大分数对应的删除方案

	rmBlocks      []bool  // 当前删除方案
	inversedIndex int     // 当前删除方案是从上一次的方案改动哪个flag而来的
	lastScore     float64 // 上一次方案的分数
}

type objectBlock struct {
	Index     int
	StorageID cdssdk.StorageID
	HasEntity bool            // 节点拥有实际的文件数据块
	HasShadow bool            // 如果节点拥有完整文件数据，那么认为这个节点拥有所有块，这些块被称为影子块
	FileHash  cdssdk.FileHash // 只有在拥有实际文件数据块时，这个字段才有值
}

type stgDist struct {
	StorageID cdssdk.StorageID
	Distance  float64
}

type combinatorialTree struct {
	nodes             []combinatorialTreeNode
	blocksMaps        map[int]bitmap.Bitmap64
	stgIDToLocalStgID map[cdssdk.StorageID]int
	localStgIDToStgID []cdssdk.StorageID
}

type annealingObject struct {
	totalBlockCount int
	minBlockCnt     int
	pinnedAt        []cdssdk.StorageID
	blocks          []stgmod.ObjectBlock
}

const (
	iterActionNone  = 0
	iterActionSkip  = 1
	iterActionBreak = 2
)

func newCombinatorialTree(stgBlocksMaps map[cdssdk.StorageID]*bitmap.Bitmap64) combinatorialTree {
	tree := combinatorialTree{
		blocksMaps:        make(map[int]bitmap.Bitmap64),
		stgIDToLocalStgID: make(map[cdssdk.StorageID]int),
	}

	tree.nodes = make([]combinatorialTreeNode, (1 << len(stgBlocksMaps)))
	for id, mp := range stgBlocksMaps {
		tree.stgIDToLocalStgID[id] = len(tree.localStgIDToStgID)
		tree.blocksMaps[len(tree.localStgIDToStgID)] = *mp
		tree.localStgIDToStgID = append(tree.localStgIDToStgID, id)
	}

	tree.nodes[0].localHubID = -1
	index := 1
	tree.initNode(0, &tree.nodes[0], &index)

	return tree
}

func (t *combinatorialTree) initNode(minAvaiLocalHubID int, parent *combinatorialTreeNode, index *int) {
	for i := minAvaiLocalHubID; i < len(t.stgIDToLocalStgID); i++ {
		curIndex := *index
		*index++
		bitMp := t.blocksMaps[i]
		bitMp.Or(&parent.blocksBitmap)

		t.nodes[curIndex] = combinatorialTreeNode{
			localHubID:   i,
			parent:       parent,
			blocksBitmap: bitMp,
		}
		t.initNode(i+1, &t.nodes[curIndex], index)
	}
}

// 获得索引指定的节点所在的层
func (t *combinatorialTree) GetDepth(index int) int {
	depth := 0

	// 反复判断节点在哪个子树。从左到右，子树节点的数量呈现8 4 2的变化，由此可以得到每个子树的索引值的范围
	subTreeCount := 1 << len(t.stgIDToLocalStgID)
	for index > 0 {
		if index < subTreeCount {
			// 定位到一个子树后，深度+1，然后进入这个子树，使用同样的方法再进行定位。
			// 进入子树后需要将索引值-1，因为要去掉子树的根节点
			index--
			depth++
		} else {
			// 如果索引值不在这个子树范围内，则将值减去子树的节点数量，
			// 这样每一次都可以视为使用同样的逻辑对不同大小的树进行判断。
			index -= subTreeCount
		}
		subTreeCount >>= 1
	}

	return depth
}

// 更新某一个算力中心节点的块分布位图，同时更新它对应组合树节点的所有子节点。
// 如果更新到某个节点时，已有K个块，那么就不会再更新它的子节点
func (t *combinatorialTree) UpdateBitmap(stgID cdssdk.StorageID, mp bitmap.Bitmap64, k int) {
	t.blocksMaps[t.stgIDToLocalStgID[stgID]] = mp
	// 首先定义两种遍历树节点时的移动方式：
	//  1. 竖直移动（深度增加）：从一个节点移动到它最左边的子节点。每移动一步，index+1
	//  2. 水平移动：从一个节点移动到它右边的兄弟节点。每移动一步，根据它所在的深度，index+8，+4，+2
	// LocalID从0开始，将其+1后得到移动步数steps。
	// 将移动步数拆成多部分，分配到上述的两种移动方式上，并进行任意组合，且保证第一次为至少进行一次的竖直移动，移动之后的节点都会是同一个计算中心节点。
	steps := t.stgIDToLocalStgID[stgID] + 1
	for d := 1; d <= steps; d++ {
		t.iterCombBits(len(t.stgIDToLocalStgID)-1, steps-d, 0, func(i int) {
			index := d + i
			node := &t.nodes[index]

			newMp := t.blocksMaps[node.localHubID]
			newMp.Or(&node.parent.blocksBitmap)
			node.blocksBitmap = newMp
			if newMp.Weight() >= k {
				return
			}

			t.iterChildren(index, func(index, parentIndex, depth int) int {
				curNode := &t.nodes[index]
				parentNode := t.nodes[parentIndex]

				newMp := t.blocksMaps[curNode.localHubID]
				newMp.Or(&parentNode.blocksBitmap)
				curNode.blocksBitmap = newMp
				if newMp.Weight() >= k {
					return iterActionSkip
				}

				return iterActionNone
			})
		})
	}
}

// 遍历树，找到至少拥有K个块的树节点的最大深度
func (t *combinatorialTree) FindKBlocksMaxDepth(k int) int {
	maxDepth := -1
	t.iterChildren(0, func(index, parentIndex, depth int) int {
		if t.nodes[index].blocksBitmap.Weight() >= k {
			if maxDepth < depth {
				maxDepth = depth
			}
			return iterActionSkip
		}
		// 如果到了叶子节点，还没有找到K个块，那就认为要满足K个块，至少需要再多一个节点，即深度+1。
		// 由于遍历时采用的是深度优先的算法，因此遍历到这个叶子节点时，叶子节点再加一个节点的组合已经在前面搜索过，
		// 所以用当前叶子节点深度+1来作为当前分支的结果就可以，即使当前情况下增加任意一个节点依然不够K块，
		// 可以使用同样的思路去递推到当前叶子节点增加两个块的情况。
		if t.nodes[index].localHubID == len(t.stgIDToLocalStgID)-1 {
			if maxDepth < depth+1 {
				maxDepth = depth + 1
			}
		}

		return iterActionNone
	})

	if maxDepth == -1 || maxDepth > len(t.stgIDToLocalStgID) {
		return len(t.stgIDToLocalStgID)
	}

	return maxDepth
}

func (t *combinatorialTree) iterCombBits(width int, count int, offset int, callback func(int)) {
	if count == 0 {
		callback(offset)
		return
	}

	for b := width; b >= count; b-- {
		t.iterCombBits(b-1, count-1, offset+(1<<b), callback)
	}
}

func (t *combinatorialTree) iterChildren(index int, do func(index int, parentIndex int, depth int) int) {
	curNode := &t.nodes[index]
	childIndex := index + 1
	curDepth := t.GetDepth(index)

	childCounts := len(t.stgIDToLocalStgID) - 1 - curNode.localHubID
	if childCounts == 0 {
		return
	}

	childTreeNodeCnt := 1 << (childCounts - 1)
	for c := 0; c < childCounts; c++ {
		act := t.itering(childIndex, index, curDepth+1, do)
		if act == iterActionBreak {
			return
		}

		childIndex += childTreeNodeCnt
		childTreeNodeCnt >>= 1
	}
}

func (t *combinatorialTree) itering(index int, parentIndex int, depth int, do func(index int, parentIndex int, depth int) int) int {
	act := do(index, parentIndex, depth)
	if act == iterActionBreak {
		return act
	}
	if act == iterActionSkip {
		return iterActionNone
	}

	curNode := &t.nodes[index]
	childIndex := index + 1

	childCounts := len(t.stgIDToLocalStgID) - 1 - curNode.localHubID
	if childCounts == 0 {
		return iterActionNone
	}

	childTreeNodeCnt := 1 << (childCounts - 1)
	for c := 0; c < childCounts; c++ {
		act = t.itering(childIndex, index, depth+1, do)
		if act == iterActionBreak {
			return act
		}

		childIndex += childTreeNodeCnt
		childTreeNodeCnt >>= 1
	}

	return iterActionNone
}

type combinatorialTreeNode struct {
	localHubID   int
	parent       *combinatorialTreeNode
	blocksBitmap bitmap.Bitmap64 // 选择了这个中心之后，所有中心一共包含多少种块
}

type annealingSolution struct {
	blockList []objectBlock // 所有节点的块分布情况
	rmBlocks  []bool        // 要删除哪些块
}

func (t *CleanPinned) startAnnealing(allStgInfos map[cdssdk.StorageID]*stgmod.StorageDetail, readerStgIDs []cdssdk.StorageID, object annealingObject) annealingSolution {
	state := &annealingState{
		allStgInfos:        allStgInfos,
		readerStgIDs:       readerStgIDs,
		stgsSortedByReader: make(map[cdssdk.StorageID][]stgDist),
		object:             object,
		stgBlockBitmaps:    make(map[cdssdk.StorageID]*bitmap.Bitmap64),
	}

	t.initBlockList(state)
	if state.blockList == nil {
		return annealingSolution{}
	}

	t.initNodeBlockBitmap(state)

	t.sortNodeByReaderDistance(state)

	state.rmBlocks = make([]bool, len(state.blockList))
	state.inversedIndex = -1
	state.stgCombTree = newCombinatorialTree(state.stgBlockBitmaps)

	state.lastScore = t.calcScore(state)
	state.maxScore = state.lastScore
	state.maxScoreRmBlocks = lo2.ArrayClone(state.rmBlocks)

	// 模拟退火算法的温度
	curTemp := state.lastScore
	// 结束温度
	finalTemp := curTemp * 0.2
	// 冷却率
	coolingRate := 0.95

	for curTemp > finalTemp {
		state.inversedIndex = rand.Intn(len(state.rmBlocks))
		block := state.blockList[state.inversedIndex]
		state.rmBlocks[state.inversedIndex] = !state.rmBlocks[state.inversedIndex]
		state.stgBlockBitmaps[block.StorageID].Set(block.Index, !state.rmBlocks[state.inversedIndex])
		state.stgCombTree.UpdateBitmap(block.StorageID, *state.stgBlockBitmaps[block.StorageID], state.object.minBlockCnt)

		curScore := t.calcScore(state)

		dScore := curScore - state.lastScore
		// 如果新方案比旧方案得分低，且没有要求强制接受新方案，那么就将变化改回去
		if curScore == 0 || (dScore < 0 && !t.alwaysAccept(curTemp, dScore, coolingRate)) {
			state.rmBlocks[state.inversedIndex] = !state.rmBlocks[state.inversedIndex]
			state.stgBlockBitmaps[block.StorageID].Set(block.Index, !state.rmBlocks[state.inversedIndex])
			state.stgCombTree.UpdateBitmap(block.StorageID, *state.stgBlockBitmaps[block.StorageID], state.object.minBlockCnt)
			// fmt.Printf("\n")
		} else {
			// fmt.Printf(" accept!\n")
			state.lastScore = curScore
			if state.maxScore < curScore {
				state.maxScore = state.lastScore
				state.maxScoreRmBlocks = lo2.ArrayClone(state.rmBlocks)
			}
		}
		curTemp *= coolingRate
	}
	// fmt.Printf("final: %v\n", state.maxScoreRmBlocks)
	return annealingSolution{
		blockList: state.blockList,
		rmBlocks:  state.maxScoreRmBlocks,
	}
}

func (t *CleanPinned) initBlockList(ctx *annealingState) {
	blocksMap := make(map[cdssdk.StorageID][]objectBlock)

	// 先生成所有的影子块
	for _, pinned := range ctx.object.pinnedAt {
		blocks := make([]objectBlock, 0, ctx.object.totalBlockCount)
		for i := 0; i < ctx.object.totalBlockCount; i++ {
			blocks = append(blocks, objectBlock{
				Index:     i,
				StorageID: pinned,
				HasShadow: true,
			})
		}
		blocksMap[pinned] = blocks
	}

	// 再填充实际块
	for _, b := range ctx.object.blocks {
		blocks := blocksMap[b.StorageID]

		has := false
		for i := range blocks {
			if blocks[i].Index == b.Index {
				blocks[i].HasEntity = true
				blocks[i].FileHash = b.FileHash
				has = true
				break
			}
		}

		if has {
			continue
		}

		blocks = append(blocks, objectBlock{
			Index:     b.Index,
			StorageID: b.StorageID,
			HasEntity: true,
			FileHash:  b.FileHash,
		})
		blocksMap[b.StorageID] = blocks
	}

	var sortedBlocks []objectBlock
	for _, bs := range blocksMap {
		sortedBlocks = append(sortedBlocks, bs...)
	}
	sortedBlocks = sort2.Sort(sortedBlocks, func(left objectBlock, right objectBlock) int {
		d := left.StorageID - right.StorageID
		if d != 0 {
			return int(d)
		}

		return left.Index - right.Index
	})

	ctx.blockList = sortedBlocks
}

func (t *CleanPinned) initNodeBlockBitmap(state *annealingState) {
	for _, b := range state.blockList {
		mp, ok := state.stgBlockBitmaps[b.StorageID]
		if !ok {
			nb := bitmap.Bitmap64(0)
			mp = &nb
			state.stgBlockBitmaps[b.StorageID] = mp
		}
		mp.Set(b.Index, true)
	}
}

func (t *CleanPinned) sortNodeByReaderDistance(state *annealingState) {
	for _, r := range state.readerStgIDs {
		var nodeDists []stgDist

		for n := range state.stgBlockBitmaps {
			if r == n {
				// 同节点时距离视为0.1
				nodeDists = append(nodeDists, stgDist{
					StorageID: n,
					Distance:  consts.StorageDistanceSameStorage,
				})
			} else if state.allStgInfos[r].MasterHub.LocationID == state.allStgInfos[n].MasterHub.LocationID {
				// 同地区时距离视为1
				nodeDists = append(nodeDists, stgDist{
					StorageID: n,
					Distance:  consts.StorageDistanceSameLocation,
				})
			} else {
				// 不同地区时距离视为5
				nodeDists = append(nodeDists, stgDist{
					StorageID: n,
					Distance:  consts.StorageDistanceOther,
				})
			}
		}

		state.stgsSortedByReader[r] = sort2.Sort(nodeDists, func(left, right stgDist) int { return sort2.Cmp(left.Distance, right.Distance) })
	}
}

func (t *CleanPinned) calcScore(state *annealingState) float64 {
	dt := t.calcDisasterTolerance(state)
	ac := t.calcMinAccessCost(state)
	sc := t.calcSpaceCost(state)

	dtSc := 1.0
	if dt < 1 {
		dtSc = 0
	} else if dt >= 2 {
		dtSc = 1.5
	}

	newSc := 0.0
	if dt == 0 || ac == 0 {
		newSc = 0
	} else {
		newSc = dtSc / (sc * ac)
	}

	// fmt.Printf("solu: %v, cur: %v, dt: %v, ac: %v, sc: %v \n", state.rmBlocks, newSc, dt, ac, sc)
	return newSc
}

// 计算容灾度
func (t *CleanPinned) calcDisasterTolerance(state *annealingState) float64 {
	if state.inversedIndex != -1 {
		node := state.blockList[state.inversedIndex]
		state.stgCombTree.UpdateBitmap(node.StorageID, *state.stgBlockBitmaps[node.StorageID], state.object.minBlockCnt)
	}
	return float64(len(state.stgBlockBitmaps) - state.stgCombTree.FindKBlocksMaxDepth(state.object.minBlockCnt))
}

// 计算最小访问数据的代价
func (t *CleanPinned) calcMinAccessCost(state *annealingState) float64 {
	cost := math.MaxFloat64
	for _, reader := range state.readerStgIDs {
		tarNodes := state.stgsSortedByReader[reader]
		gotBlocks := bitmap.Bitmap64(0)
		thisCost := 0.0

		for _, tar := range tarNodes {
			tarNodeMp := state.stgBlockBitmaps[tar.StorageID]

			// 只需要从目的节点上获得缺少的块
			curWeigth := gotBlocks.Weight()
			// 下面的if会在拿到k个块之后跳出循环，所以or多了块也没关系
			gotBlocks.Or(tarNodeMp)
			// 但是算读取块的消耗时，不能多算，最多算读了k个块的消耗
			willGetBlocks := math2.Min(gotBlocks.Weight()-curWeigth, state.object.minBlockCnt-curWeigth)
			thisCost += float64(willGetBlocks) * float64(tar.Distance)

			if gotBlocks.Weight() >= state.object.minBlockCnt {
				break
			}
		}
		if gotBlocks.Weight() >= state.object.minBlockCnt {
			cost = math.Min(cost, thisCost)
		}
	}

	return cost
}

// 计算冗余度
func (t *CleanPinned) calcSpaceCost(ctx *annealingState) float64 {
	blockCount := 0
	for i, b := range ctx.blockList {
		if ctx.rmBlocks[i] {
			continue
		}

		if b.HasEntity {
			blockCount++
		}
		if b.HasShadow {
			blockCount++
		}
	}
	// 所有算力中心上拥有的块的总数 / 一个对象被分成了几个块
	return float64(blockCount) / float64(ctx.object.minBlockCnt)
}

// 如果新方案得分比旧方案小，那么在一定概率内也接受新方案
func (t *CleanPinned) alwaysAccept(curTemp float64, dScore float64, coolingRate float64) bool {
	v := math.Exp(dScore / curTemp / coolingRate)
	// fmt.Printf(" -- chance: %v, temp: %v", v, curTemp)
	return v > rand.Float64()
}

func (t *CleanPinned) makePlansForRepObject(allStgInfos map[cdssdk.StorageID]*stgmod.StorageDetail, solu annealingSolution, obj stgmod.ObjectDetail, planBld *exec.PlanBuilder, planningHubIDs map[cdssdk.StorageID]bool) coormq.UpdatingObjectRedundancy {
	entry := coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: obj.Object.Redundancy,
	}

	for i, f := range solu.rmBlocks {
		hasCache := lo.ContainsBy(obj.Blocks, func(b stgmod.ObjectBlock) bool { return b.StorageID == solu.blockList[i].StorageID }) ||
			lo.ContainsBy(obj.PinnedAt, func(n cdssdk.StorageID) bool { return n == solu.blockList[i].StorageID })
		willRm := f

		if !willRm {
			// 如果对象在退火后要保留副本的节点没有副本，则需要在这个节点创建副本
			if !hasCache {
				ft := ioswitch2.NewFromTo()

				fromStg := allStgInfos[obj.Blocks[0].StorageID]
				ft.AddFrom(ioswitch2.NewFromShardstore(obj.Object.FileHash, *fromStg.MasterHub, fromStg.Storage, ioswitch2.RawStream()))
				toStg := allStgInfos[solu.blockList[i].StorageID]
				ft.AddTo(ioswitch2.NewToShardStore(*toStg.MasterHub, *toStg, ioswitch2.RawStream(), fmt.Sprintf("%d.0", obj.Object.ObjectID)))

				err := parser.Parse(ft, planBld)
				if err != nil {
					// TODO 错误处理
					continue
				}
				planningHubIDs[solu.blockList[i].StorageID] = true
			}
			entry.Blocks = append(entry.Blocks, stgmod.ObjectBlock{
				ObjectID:  obj.Object.ObjectID,
				Index:     solu.blockList[i].Index,
				StorageID: solu.blockList[i].StorageID,
				FileHash:  obj.Object.FileHash,
			})
		}
	}

	return entry
}

func (t *CleanPinned) makePlansForECObject(allStgInfos map[cdssdk.StorageID]*stgmod.StorageDetail, solu annealingSolution, obj stgmod.ObjectDetail, planBld *exec.PlanBuilder, planningHubIDs map[cdssdk.StorageID]bool) coormq.UpdatingObjectRedundancy {
	entry := coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: obj.Object.Redundancy,
	}

	reconstrct := make(map[cdssdk.StorageID]*[]int)
	for i, f := range solu.rmBlocks {
		block := solu.blockList[i]
		if !f {
			entry.Blocks = append(entry.Blocks, stgmod.ObjectBlock{
				ObjectID:  obj.Object.ObjectID,
				Index:     block.Index,
				StorageID: block.StorageID,
				FileHash:  block.FileHash,
			})

			// 如果这个块是影子块，那么就要从完整对象里重建这个块
			if !block.HasEntity {
				re, ok := reconstrct[block.StorageID]
				if !ok {
					re = &[]int{}
					reconstrct[block.StorageID] = re
				}

				*re = append(*re, block.Index)
			}
		}
	}

	ecRed := obj.Object.Redundancy.(*cdssdk.ECRedundancy)

	for id, idxs := range reconstrct {
		ft := ioswitch2.NewFromTo()
		ft.ECParam = ecRed
		ft.AddFrom(ioswitch2.NewFromShardstore(obj.Object.FileHash, *allStgInfos[id].MasterHub, allStgInfos[id].Storage, ioswitch2.RawStream()))

		for _, i := range *idxs {
			ft.AddTo(ioswitch2.NewToShardStore(*allStgInfos[id].MasterHub, *allStgInfos[id], ioswitch2.ECSrteam(i), fmt.Sprintf("%d.%d", obj.Object.ObjectID, i)))
		}

		err := parser.Parse(ft, planBld)
		if err != nil {
			// TODO 错误处理
			continue
		}

		planningHubIDs[id] = true
	}
	return entry
}

func (t *CleanPinned) executePlans(ctx ExecuteContext, planBld *exec.PlanBuilder, planningStgIDs map[cdssdk.StorageID]bool) (map[string]exec.VarValue, error) {
	// 统一加锁，有重复也没关系
	lockBld := reqbuilder.NewBuilder()
	for id := range planningStgIDs {
		lockBld.Shard().Buzy(id)
	}
	lock, err := lockBld.MutexLock(ctx.Args.DistLock)
	if err != nil {
		return nil, fmt.Errorf("acquiring distlock: %w", err)
	}
	defer lock.Unlock()

	wg := sync.WaitGroup{}

	// 执行IO计划
	var ioSwRets map[string]exec.VarValue
	var ioSwErr error
	wg.Add(1)
	go func() {
		defer wg.Done()

		execCtx := exec.NewExecContext()
		exec.SetValueByType(execCtx, ctx.Args.StgMgr)
		ret, err := planBld.Execute(execCtx).Wait(context.TODO())
		if err != nil {
			ioSwErr = fmt.Errorf("executing io switch plan: %w", err)
			return
		}
		ioSwRets = ret
	}()

	wg.Wait()

	if ioSwErr != nil {
		return nil, ioSwErr
	}

	return ioSwRets, nil
}

func (t *CleanPinned) populateECObjectEntry(entry *coormq.UpdatingObjectRedundancy, obj stgmod.ObjectDetail, ioRets map[string]exec.VarValue) {
	for i := range entry.Blocks {
		if entry.Blocks[i].FileHash != "" {
			continue
		}

		key := fmt.Sprintf("%d.%d", obj.Object.ObjectID, entry.Blocks[i].Index)
		// 不应该出现key不存在的情况
		entry.Blocks[i].FileHash = ioRets[key].(*ops2.FileHashValue).Hash
	}
}

func init() {
	RegisterMessageConvertor(NewCleanPinned)
}
