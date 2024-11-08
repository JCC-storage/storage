package parser

import (
	"fmt"
	"math"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type DefaultParser struct {
	EC cdssdk.ECRedundancy
}

func NewParser(ec cdssdk.ECRedundancy) *DefaultParser {
	return &DefaultParser{
		EC: ec,
	}
}

type IndexedStream struct {
	Stream    *dag.Var
	DataIndex int
}

type ParseContext struct {
	Ft  ioswitch2.FromTo
	DAG *ops2.GraphNodeBuilder
	// 为了产生所有To所需的数据范围，而需要From打开的范围。
	// 这个范围是基于整个文件的，且上下界都取整到条带大小的整数倍，因此上界是有可能超过文件大小的。
	ToNodes        map[ioswitch2.To]ops2.ToNode
	IndexedStreams []IndexedStream
	StreamRange    exec.Range
}

func (p *DefaultParser) Parse(ft ioswitch2.FromTo, blder *exec.PlanBuilder) error {
	ctx := ParseContext{
		Ft:      ft,
		DAG:     ops2.NewGraphNodeBuilder(),
		ToNodes: make(map[ioswitch2.To]ops2.ToNode),
	}

	// 分成两个阶段：
	// 1. 基于From和To生成更多指令，初步匹配to的需求

	// 计算一下打开流的范围
	p.calcStreamRange(&ctx)

	err := p.extend(&ctx)
	if err != nil {
		return err
	}

	// 2. 优化上一步生成的指令

	// 对于删除指令的优化，需要反复进行，直到没有变化为止。
	// 从目前实现上来说不会死循环
	for {
		opted := false
		if p.removeUnusedJoin(&ctx) {
			opted = true
		}
		if p.removeUnusedMultiplyOutput(&ctx) {
			opted = true
		}
		if p.removeUnusedSplit(&ctx) {
			opted = true
		}
		if p.omitSplitJoin(&ctx) {
			opted = true
		}

		if !opted {
			break
		}
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for p.pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	p.dropUnused(&ctx)
	p.storeIPFSWriteResult(&ctx)
	p.generateClone(&ctx)
	p.generateRange(&ctx)

	return plan.Generate(ctx.DAG.Graph, blder)
}
func (p *DefaultParser) findOutputStream(ctx *ParseContext, streamIndex int) *dag.Var {
	var ret *dag.Var
	for _, s := range ctx.IndexedStreams {
		if s.DataIndex == streamIndex {
			ret = s.Stream
			break
		}
	}
	return ret
}

// 计算输入流的打开范围。会把流的范围按条带大小取整
func (p *DefaultParser) calcStreamRange(ctx *ParseContext) {
	stripSize := int64(p.EC.ChunkSize * p.EC.K)

	rng := exec.Range{
		Offset: math.MaxInt64,
	}

	for _, to := range ctx.Ft.Toes {
		if to.GetDataIndex() == -1 {
			toRng := to.GetRange()
			rng.ExtendStart(math2.Floor(toRng.Offset, stripSize))
			if toRng.Length != nil {
				rng.ExtendEnd(math2.Ceil(toRng.Offset+*toRng.Length, stripSize))
			} else {
				rng.Length = nil
			}

		} else {
			toRng := to.GetRange()

			blkStartIndex := math2.FloorDiv(toRng.Offset, int64(p.EC.ChunkSize))
			rng.ExtendStart(blkStartIndex * stripSize)
			if toRng.Length != nil {
				blkEndIndex := math2.CeilDiv(toRng.Offset+*toRng.Length, int64(p.EC.ChunkSize))
				rng.ExtendEnd(blkEndIndex * stripSize)
			} else {
				rng.Length = nil
			}
		}
	}

	ctx.StreamRange = rng
}

func (p *DefaultParser) extend(ctx *ParseContext) error {
	for _, fr := range ctx.Ft.Froms {
		frNode, err := p.buildFromNode(ctx, fr)
		if err != nil {
			return err
		}

		ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
			Stream:    frNode.Output().Var,
			DataIndex: fr.GetDataIndex(),
		})

		// 对于完整文件的From，生成Split指令
		if fr.GetDataIndex() == -1 {
			splitNode := ctx.DAG.NewChunkedSplit(p.EC.ChunkSize)
			splitNode.Split(frNode.Output().Var, p.EC.K)
			for i := 0; i < p.EC.K; i++ {
				ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
					Stream:    splitNode.SubStream(i),
					DataIndex: i,
				})
			}
		}
	}

	// 如果有K个不同的文件块流，则生成Multiply指令，同时针对其生成的流，生成Join指令
	ecInputStrs := make(map[int]*dag.Var)
	for _, s := range ctx.IndexedStreams {
		if s.DataIndex >= 0 && ecInputStrs[s.DataIndex] == nil {
			ecInputStrs[s.DataIndex] = s.Stream
			if len(ecInputStrs) == p.EC.K {
				break
			}
		}
	}

	if len(ecInputStrs) == p.EC.K {
		mulNode := ctx.DAG.NewECMultiply(p.EC)

		for i, s := range ecInputStrs {
			mulNode.AddInput(s, i)
		}
		for i := 0; i < p.EC.N; i++ {
			ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
				Stream:    mulNode.NewOutput(i),
				DataIndex: i,
			})
		}

		joinNode := ctx.DAG.NewChunkedJoin(p.EC.ChunkSize)
		for i := 0; i < p.EC.K; i++ {
			// 不可能找不到流
			joinNode.AddInput(p.findOutputStream(ctx, i))
		}
		ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
			Stream:    joinNode.Joined(),
			DataIndex: -1,
		})
	}

	// 为每一个To找到一个输入流
	for _, to := range ctx.Ft.Toes {
		toNode, err := p.buildToNode(ctx, to)
		if err != nil {
			return err
		}
		ctx.ToNodes[to] = toNode

		str := p.findOutputStream(ctx, to.GetDataIndex())
		if str == nil {
			return fmt.Errorf("no output stream found for data index %d", to.GetDataIndex())
		}

		toNode.SetInput(str)
	}

	return nil
}

func (p *DefaultParser) buildFromNode(ctx *ParseContext, f ioswitch2.From) (ops2.FromNode, error) {
	var repRange exec.Range
	var blkRange exec.Range

	repRange.Offset = ctx.StreamRange.Offset
	blkRange.Offset = ctx.StreamRange.Offset / int64(p.EC.ChunkSize*p.EC.K) * int64(p.EC.ChunkSize)
	if ctx.StreamRange.Length != nil {
		repRngLen := *ctx.StreamRange.Length
		repRange.Length = &repRngLen

		blkRngLen := *ctx.StreamRange.Length / int64(p.EC.ChunkSize*p.EC.K) * int64(p.EC.ChunkSize)
		blkRange.Length = &blkRngLen
	}

	switch f := f.(type) {
	case *ioswitch2.FromShardstore:
		t := ctx.DAG.NewShardRead(f.Storage.StorageID, types.NewOpen(f.FileHash))

		if f.DataIndex == -1 {
			t.Open.WithNullableLength(repRange.Offset, repRange.Length)
		} else {
			t.Open.WithNullableLength(blkRange.Offset, blkRange.Length)
		}

		switch addr := f.Hub.Address.(type) {
		case *cdssdk.HttpAddressInfo:
			t.Env().ToEnvWorker(&ioswitch2.HttpHubWorker{Hub: f.Hub})
			t.Env().Pinned = true

		case *cdssdk.GRPCAddressInfo:
			t.Env().ToEnvWorker(&ioswitch2.AgentWorker{Hub: f.Hub, Address: *addr})
			t.Env().Pinned = true

		default:
			return nil, fmt.Errorf("unsupported node address type %T", addr)
		}

		return t, nil

	case *ioswitch2.FromDriver:
		n := ctx.DAG.NewFromDriver(f.Handle)
		n.Env().ToEnvDriver()
		n.Env().Pinned = true

		if f.DataIndex == -1 {
			f.Handle.RangeHint.Offset = repRange.Offset
			f.Handle.RangeHint.Length = repRange.Length
		} else {
			f.Handle.RangeHint.Offset = blkRange.Offset
			f.Handle.RangeHint.Length = blkRange.Length
		}

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported from type %T", f)
	}
}

func (p *DefaultParser) buildToNode(ctx *ParseContext, t ioswitch2.To) (ops2.ToNode, error) {
	switch t := t.(type) {
	case *ioswitch2.ToShardStore:
		n := ctx.DAG.NewShardWrite(t.Storage.StorageID, t.FileHashStoreKey)

		switch addr := t.Hub.Address.(type) {
		case *cdssdk.HttpAddressInfo:
			n.Env().ToEnvWorker(&ioswitch2.HttpHubWorker{Hub: t.Hub})

		case *cdssdk.GRPCAddressInfo:
			n.Env().ToEnvWorker(&ioswitch2.AgentWorker{Hub: t.Hub, Address: *addr})

		default:
			return nil, fmt.Errorf("unsupported node address type %T", addr)
		}

		n.Env().Pinned = true

		return n, nil

	case *ioswitch2.ToDriver:
		n := ctx.DAG.NewToDriver(t.Handle)
		n.Env().ToEnvDriver()
		n.Env().Pinned = true

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported to type %T", t)
	}
}

// 删除输出流未被使用的Join指令
func (p *DefaultParser) removeUnusedJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ops2.ChunkedJoinNode](ctx.DAG.Graph, func(node *ops2.ChunkedJoinNode) bool {
		if node.InputStreams().Len() > 0 {
			return true
		}

		node.RemoveAllInputs()
		ctx.DAG.RemoveNode(node)
		return true
	})

	return changed
}

// 减少未使用的Multiply指令的输出流。如果减少到0，则删除该指令
func (p *DefaultParser) removeUnusedMultiplyOutput(ctx *ParseContext) bool {
	changed := false
	dag.WalkOnlyType[*ops2.ECMultiplyNode](ctx.DAG.Graph, func(node *ops2.ECMultiplyNode) bool {
		outArr := node.OutputStreams().RawArray()
		for i2, out := range outArr {
			if out.To().Len() > 0 {
				continue
			}

			outArr[i2] = nil
			node.OutputIndexes[i2] = -2
			changed = true
		}
		node.OutputStreams().SetRawArray(lo2.RemoveAllDefault(outArr))
		node.OutputIndexes = lo2.RemoveAll(node.OutputIndexes, -2)

		// 如果所有输出流都被删除，则删除该指令
		if node.OutputStreams().Len() == 0 {
			node.RemoveAllInputs()
			ctx.DAG.RemoveNode(node)
			changed = true
		}

		return true
	})
	return changed
}

// 删除未使用的Split指令
func (p *DefaultParser) removeUnusedSplit(ctx *ParseContext) bool {
	changed := false
	dag.WalkOnlyType[*ops2.ChunkedSplitNode](ctx.DAG.Graph, func(typ *ops2.ChunkedSplitNode) bool {
		// Split出来的每一个流都没有被使用，才能删除这个指令
		for _, out := range typ.OutputStreams().RawArray() {
			if out.To().Len() > 0 {
				return true
			}
		}

		typ.Clear()
		ctx.DAG.RemoveNode(typ)
		changed = true
		return true
	})

	return changed
}

// 如果Split的结果被完全用于Join，则省略Split和Join指令
func (p *DefaultParser) omitSplitJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ops2.ChunkedSplitNode](ctx.DAG.Graph, func(splitNode *ops2.ChunkedSplitNode) bool {
		// Split指令的每一个输出都有且只有一个目的地
		var dstNode dag.Node
		for _, out := range splitNode.OutputStreams().RawArray() {
			if out.To().Len() != 1 {
				return true
			}

			if dstNode == nil {
				dstNode = out.To().Get(0).Node
			} else if dstNode != out.To().Get(0).Node {
				return true
			}
		}

		if dstNode == nil {
			return true
		}

		// 且这个目的地要是一个Join指令
		joinNode, ok := dstNode.(*ops2.ChunkedJoinNode)
		if !ok {
			return true
		}

		// 同时这个Join指令的输入也必须全部来自Split指令的输出。
		// 由于上面判断了Split指令的输出目的地都相同，所以这里只要判断Join指令的输入数量是否与Split指令的输出数量相同即可
		if joinNode.InputStreams().Len() != splitNode.OutputStreams().Len() {
			return true
		}

		// 所有条件都满足，可以开始省略操作，将Join操作的目的地的输入流替换为Split操作的输入流：
		// F->Split->Join->T 变换为：F->T
		splitInput := splitNode.InputStreams().Get(0)
		for _, to := range joinNode.Joined().To().RawArray() {
			splitInput.StreamTo(to.Node, to.SlotIndex)
		}
		splitInput.StreamNotTo(splitNode, 0)

		// 并删除这两个指令
		ctx.DAG.RemoveNode(joinNode)
		ctx.DAG.RemoveNode(splitNode)

		changed = true
		return true
	})

	return changed
}

// 通过流的输入输出位置来确定指令的执行位置。
// To系列的指令都会有固定的执行位置，这些位置会随着pin操作逐步扩散到整个DAG，
// 所以理论上不会出现有指令的位置始终无法确定的情况。
func (p *DefaultParser) pin(ctx *ParseContext) bool {
	changed := false
	ctx.DAG.Walk(func(node dag.Node) bool {
		if node.Env().Pinned {
			return true
		}

		var toEnv *dag.NodeEnv
		for _, out := range node.OutputStreams().RawArray() {
			for _, to := range out.To().RawArray() {
				if to.Node.Env().Type == dag.EnvUnknown {
					continue
				}

				if toEnv == nil {
					toEnv = to.Node.Env()
				} else if !toEnv.Equals(to.Node.Env()) {
					toEnv = nil
					break
				}
			}
		}

		if toEnv != nil {
			if !node.Env().Equals(toEnv) {
				changed = true
			}

			*node.Env() = *toEnv
			return true
		}

		// 否则根据输入流的始发地来固定
		var fromEnv *dag.NodeEnv
		for _, in := range node.InputStreams().RawArray() {
			if in.From().Node.Env().Type == dag.EnvUnknown {
				continue
			}

			if fromEnv == nil {
				fromEnv = in.From().Node.Env()
			} else if !fromEnv.Equals(in.From().Node.Env()) {
				fromEnv = nil
				break
			}
		}

		if fromEnv != nil {
			if !node.Env().Equals(fromEnv) {
				changed = true
			}

			*node.Env() = *fromEnv
		}
		return true
	})

	return changed
}

// 对于所有未使用的流，增加Drop指令
func (p *DefaultParser) dropUnused(ctx *ParseContext) {
	ctx.DAG.Walk(func(node dag.Node) bool {
		for _, out := range node.OutputStreams().RawArray() {
			if out.To().Len() == 0 {
				n := ctx.DAG.NewDropStream()
				*n.Env() = *node.Env()
				n.SetInput(out)
			}
		}
		return true
	})
}

// 为IPFS写入指令存储结果
func (p *DefaultParser) storeIPFSWriteResult(ctx *ParseContext) {
	dag.WalkOnlyType[*ops2.ShardWriteNode](ctx.DAG.Graph, func(n *ops2.ShardWriteNode) bool {
		if n.FileHashStoreKey == "" {
			return true
		}

		storeNode := ctx.DAG.NewStore()
		storeNode.Env().ToEnvDriver()

		storeNode.Store(n.FileHashStoreKey, n.FileHashVar())
		return true
	})
}

// 生成Range指令。StreamRange可能超过文件总大小，但Range指令会在数据量不够时不报错而是正常返回
func (p *DefaultParser) generateRange(ctx *ParseContext) {
	for i := 0; i < len(ctx.Ft.Toes); i++ {
		to := ctx.Ft.Toes[i]
		toNode := ctx.ToNodes[to]

		toDataIdx := to.GetDataIndex()
		toRng := to.GetRange()

		if toDataIdx == -1 {
			n := ctx.DAG.NewRange()
			toInput := toNode.Input()
			*n.Env() = *toInput.Var.From().Node.Env()
			rnged := n.RangeStream(toInput.Var, exec.Range{
				Offset: toRng.Offset - ctx.StreamRange.Offset,
				Length: toRng.Length,
			})
			toInput.Var.StreamNotTo(toNode, toInput.Index)
			toNode.SetInput(rnged)

		} else {
			stripSize := int64(p.EC.ChunkSize * p.EC.K)
			blkStartIdx := ctx.StreamRange.Offset / stripSize

			blkStart := blkStartIdx * int64(p.EC.ChunkSize)

			n := ctx.DAG.NewRange()
			toInput := toNode.Input()
			*n.Env() = *toInput.Var.From().Node.Env()
			rnged := n.RangeStream(toInput.Var, exec.Range{
				Offset: toRng.Offset - blkStart,
				Length: toRng.Length,
			})
			toInput.Var.StreamNotTo(toNode, toInput.Index)
			toNode.SetInput(rnged)
		}
	}
}

// 生成Clone指令
func (p *DefaultParser) generateClone(ctx *ParseContext) {
	ctx.DAG.Walk(func(node dag.Node) bool {
		for _, out := range node.OutputStreams().RawArray() {
			if out.To().Len() <= 1 {
				continue
			}

			c := ctx.DAG.NewCloneStream()
			*c.Env() = *node.Env()
			for _, to := range out.To().RawArray() {
				c.NewOutput().StreamTo(to.Node, to.SlotIndex)
			}
			out.To().Resize(0)
			c.SetInput(out)
		}

		for _, out := range node.OutputValues().RawArray() {
			if out.To().Len() <= 1 {
				continue
			}

			t := ctx.DAG.NewCloneValue()
			*t.Env() = *node.Env()
			for _, to := range out.To().RawArray() {
				t.NewOutput().ValueTo(to.Node, to.SlotIndex)
			}
			out.To().Resize(0)
			t.SetInput(out)
		}

		return true
	})
}
