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

type IndexedStream struct {
	Stream      *dag.Var
	StreamIndex ioswitch2.StreamIndex
}

type ParseContext struct {
	Ft  ioswitch2.FromTo
	DAG *ops2.GraphNodeBuilder
	// 为了产生所有To所需的数据范围，而需要From打开的范围。
	// 这个范围是基于整个文件的，且上下界都取整到条带大小的整数倍，因此上界是有可能超过文件大小的。
	ToNodes        map[ioswitch2.To]ops2.ToNode
	IndexedStreams []IndexedStream
	StreamRange    exec.Range
	UseEC          bool // 是否使用纠删码
	UseSegment     bool // 是否使用分段
}

func Parse(ft ioswitch2.FromTo, blder *exec.PlanBuilder) error {
	ctx := ParseContext{
		Ft:      ft,
		DAG:     ops2.NewGraphNodeBuilder(),
		ToNodes: make(map[ioswitch2.To]ops2.ToNode),
	}

	// 分成两个阶段：
	// 1. 基于From和To生成更多指令，初步匹配to的需求

	err := checkEncodingParams(&ctx)
	if err != nil {
		return err
	}

	// 计算一下打开流的范围
	calcStreamRange(&ctx)

	err = extend(&ctx)
	if err != nil {
		return err
	}

	// 2. 优化上一步生成的指令

	err = removeUnusedSegment(&ctx)
	if err != nil {
		return err
	}

	// 对于删除指令的优化，需要反复进行，直到没有变化为止。
	// 从目前实现上来说不会死循环
	for {
		opted := false
		if removeUnusedJoin(&ctx) {
			opted = true
		}
		if removeUnusedMultiplyOutput(&ctx) {
			opted = true
		}
		if removeUnusedSplit(&ctx) {
			opted = true
		}
		if omitSplitJoin(&ctx) {
			opted = true
		}
		if removeUnusedSegmentJoin(&ctx) {
			opted = true
		}
		if removeUnusedSegmentSplit(&ctx) {
			opted = true
		}
		if omitSegmentSplitJoin(&ctx) {
			opted = true
		}

		if !opted {
			break
		}
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	removeUnusedFromNode(&ctx)
	dropUnused(&ctx)
	storeIPFSWriteResult(&ctx)
	generateRange(&ctx)
	generateClone(&ctx)

	return plan.Generate(ctx.DAG.Graph, blder)
}
func findOutputStream(ctx *ParseContext, streamIndex ioswitch2.StreamIndex) *dag.Var {
	var ret *dag.Var
	for _, s := range ctx.IndexedStreams {
		if s.StreamIndex == streamIndex {
			ret = s.Stream
			break
		}
	}
	return ret
}

// 检查使用不同编码时参数是否设置到位
func checkEncodingParams(ctx *ParseContext) error {
	for _, f := range ctx.Ft.Froms {
		if f.GetStreamIndex().IsEC() {
			ctx.UseEC = true
			if ctx.Ft.ECParam == nil {
				return fmt.Errorf("EC encoding parameters not set")
			}
		}

		if f.GetStreamIndex().IsSegment() {
			ctx.UseSegment = true
			if ctx.Ft.SegmentParam == nil {
				return fmt.Errorf("segment parameters not set")
			}
		}
	}

	for _, t := range ctx.Ft.Toes {
		if t.GetStreamIndex().IsEC() {
			ctx.UseEC = true
			if ctx.Ft.ECParam == nil {
				return fmt.Errorf("EC encoding parameters not set")
			}
		}

		if t.GetStreamIndex().IsSegment() {
			ctx.UseSegment = true
			if ctx.Ft.SegmentParam == nil {
				return fmt.Errorf("segment parameters not set")
			}
		}
	}

	return nil
}

// 计算输入流的打开范围。如果From或者To中包含EC的流，则会将打开范围扩大到条带大小的整数倍。
func calcStreamRange(ctx *ParseContext) {
	rng := exec.NewRange(math.MaxInt64, 0)

	for _, to := range ctx.Ft.Toes {
		strIdx := to.GetStreamIndex()
		if strIdx.IsRaw() {
			toRng := to.GetRange()
			rng.ExtendStart(toRng.Offset)
			if toRng.Length != nil {
				rng.ExtendEnd(toRng.Offset + *toRng.Length)
			} else {
				rng.Length = nil
			}
		} else if strIdx.IsEC() {
			toRng := to.GetRange()
			stripSize := ctx.Ft.ECParam.StripSize()
			blkStartIndex := math2.FloorDiv(toRng.Offset, int64(ctx.Ft.ECParam.ChunkSize))
			rng.ExtendStart(blkStartIndex * stripSize)
			if toRng.Length != nil {
				blkEndIndex := math2.CeilDiv(toRng.Offset+*toRng.Length, int64(ctx.Ft.ECParam.ChunkSize))
				rng.ExtendEnd(blkEndIndex * stripSize)
			} else {
				rng.Length = nil
			}

		} else if strIdx.IsSegment() {
			// Segment节点的Range是相对于本段的，需要加上本段的起始位置
			toRng := to.GetRange()

			segStart := ctx.Ft.SegmentParam.CalcSegmentStart(strIdx.Index)

			offset := toRng.Offset + segStart

			rng.ExtendStart(offset)
			if toRng.Length != nil {
				rng.ExtendEnd(offset + *toRng.Length)
			} else {
				rng.Length = nil
			}
		}
	}

	if ctx.UseEC {
		stripSize := ctx.Ft.ECParam.StripSize()
		rng.ExtendStart(math2.Floor(rng.Offset, stripSize))
		if rng.Length != nil {
			rng.ExtendEnd(math2.Ceil(rng.Offset+*rng.Length, stripSize))
		}
	}

	ctx.StreamRange = rng
}

func extend(ctx *ParseContext) error {
	for _, fr := range ctx.Ft.Froms {
		frNode, err := buildFromNode(ctx, fr)
		if err != nil {
			return err
		}

		ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
			Stream:      frNode.Output().Var,
			StreamIndex: fr.GetStreamIndex(),
		})

		// 对于完整文件的From，生成Split指令
		if fr.GetStreamIndex().IsRaw() {
			// 只有输入输出需要EC编码的块时，才生成相关指令
			if ctx.UseEC {
				splitNode := ctx.DAG.NewChunkedSplit(ctx.Ft.ECParam.ChunkSize)
				splitNode.Split(frNode.Output().Var, ctx.Ft.ECParam.K)
				for i := 0; i < ctx.Ft.ECParam.K; i++ {
					ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
						Stream:      splitNode.SubStream(i),
						StreamIndex: ioswitch2.ECSrteam(i),
					})
				}
			}

			// 同上
			if ctx.UseSegment {
				splitNode := ctx.DAG.NewSegmentSplit(ctx.Ft.SegmentParam.Segments)
				splitNode.SetInput(frNode.Output().Var)
				for i := 0; i < len(ctx.Ft.SegmentParam.Segments); i++ {
					ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
						Stream:      splitNode.Segment(i),
						StreamIndex: ioswitch2.SegmentStream(i),
					})
				}
			}
		}
	}

	if ctx.UseEC {
		// 如果有K个不同的文件块流，则生成Multiply指令，同时针对其生成的流，生成Join指令
		ecInputStrs := make(map[int]*dag.Var)
		for _, s := range ctx.IndexedStreams {
			if s.StreamIndex.IsEC() && ecInputStrs[s.StreamIndex.Index] == nil {
				ecInputStrs[s.StreamIndex.Index] = s.Stream
				if len(ecInputStrs) == ctx.Ft.ECParam.K {
					break
				}
			}
		}

		if len(ecInputStrs) == ctx.Ft.ECParam.K {
			mulNode := ctx.DAG.NewECMultiply(*ctx.Ft.ECParam)

			for i, s := range ecInputStrs {
				mulNode.AddInput(s, i)
			}
			for i := 0; i < ctx.Ft.ECParam.N; i++ {
				ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
					Stream:      mulNode.NewOutput(i),
					StreamIndex: ioswitch2.ECSrteam(i),
				})
			}

			joinNode := ctx.DAG.NewChunkedJoin(ctx.Ft.ECParam.ChunkSize)
			for i := 0; i < ctx.Ft.ECParam.K; i++ {
				// 不可能找不到流
				joinNode.AddInput(findOutputStream(ctx, ioswitch2.ECSrteam(i)))
			}
			ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
				Stream:      joinNode.Joined(),
				StreamIndex: ioswitch2.RawStream(),
			})
		}
	}

	if ctx.UseSegment {
		// 先假设有所有的顺序分段，生成Join指令，后续根据Range再实际计算是否缺少流
		joinNode := ctx.DAG.NewSegmentJoin(ctx.Ft.SegmentParam.Segments)
		for i := 0; i < ctx.Ft.SegmentParam.SegmentCount(); i++ {
			str := findOutputStream(ctx, ioswitch2.SegmentStream(i))
			if str != nil {
				joinNode.SetInput(i, str)
			}
		}
		ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
			Stream:      joinNode.Joined(),
			StreamIndex: ioswitch2.RawStream(),
		})

		// SegmentJoin生成的Join指令可以用来生成EC块
		if ctx.UseEC {
			splitNode := ctx.DAG.NewChunkedSplit(ctx.Ft.ECParam.ChunkSize)
			splitNode.Split(joinNode.Joined(), ctx.Ft.ECParam.K)

			mulNode := ctx.DAG.NewECMultiply(*ctx.Ft.ECParam)

			for i := 0; i < ctx.Ft.ECParam.K; i++ {
				mulNode.AddInput(splitNode.SubStream(i), i)
				ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
					Stream:      splitNode.SubStream(i),
					StreamIndex: ioswitch2.ECSrteam(i),
				})
			}

			for i := 0; i < ctx.Ft.ECParam.N; i++ {
				ctx.IndexedStreams = append(ctx.IndexedStreams, IndexedStream{
					Stream:      mulNode.NewOutput(i),
					StreamIndex: ioswitch2.ECSrteam(i),
				})
			}
		}
	}

	// 为每一个To找到一个输入流
	for _, to := range ctx.Ft.Toes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return err
		}
		ctx.ToNodes[to] = toNode

		str := findOutputStream(ctx, to.GetStreamIndex())
		if str == nil {
			return fmt.Errorf("no output stream found for data index %d", to.GetStreamIndex())
		}

		toNode.SetInput(str)
	}

	return nil
}

func buildFromNode(ctx *ParseContext, f ioswitch2.From) (ops2.FromNode, error) {
	var repRange exec.Range
	repRange.Offset = ctx.StreamRange.Offset
	if ctx.StreamRange.Length != nil {
		repRngLen := *ctx.StreamRange.Length
		repRange.Length = &repRngLen
	}

	var blkRange exec.Range
	if ctx.UseEC {
		blkRange.Offset = ctx.StreamRange.Offset / int64(ctx.Ft.ECParam.ChunkSize*ctx.Ft.ECParam.K) * int64(ctx.Ft.ECParam.ChunkSize)
		if ctx.StreamRange.Length != nil {
			blkRngLen := *ctx.StreamRange.Length / int64(ctx.Ft.ECParam.ChunkSize*ctx.Ft.ECParam.K) * int64(ctx.Ft.ECParam.ChunkSize)
			blkRange.Length = &blkRngLen
		}
	}

	switch f := f.(type) {
	case *ioswitch2.FromShardstore:
		t := ctx.DAG.NewShardRead(f, f.Storage.StorageID, types.NewOpen(f.FileHash))

		if f.StreamIndex.IsRaw() {
			t.Open.WithNullableLength(repRange.Offset, repRange.Length)
		} else if f.StreamIndex.IsEC() {
			t.Open.WithNullableLength(blkRange.Offset, blkRange.Length)
		} else if f.StreamIndex.IsSegment() {
			segStart := ctx.Ft.SegmentParam.CalcSegmentStart(f.StreamIndex.Index)
			segLen := ctx.Ft.SegmentParam.Segments[f.StreamIndex.Index]
			segEnd := segStart + segLen

			// 打开的范围不超过本段的范围

			openOff := ctx.StreamRange.Offset - segStart
			openOff = math2.Clamp(openOff, 0, segLen)

			openLen := segLen

			if ctx.StreamRange.Length != nil {
				strEnd := ctx.StreamRange.Offset + *ctx.StreamRange.Length
				openEnd := math2.Min(strEnd, segEnd)
				openLen = openEnd - segStart - openOff
			}

			t.Open.WithNullableLength(openOff, &openLen)
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
		n := ctx.DAG.NewFromDriver(f, f.Handle)
		n.Env().ToEnvDriver()
		n.Env().Pinned = true

		if f.StreamIndex.IsRaw() {
			f.Handle.RangeHint.Offset = repRange.Offset
			f.Handle.RangeHint.Length = repRange.Length
		} else if f.StreamIndex.IsEC() {
			f.Handle.RangeHint.Offset = blkRange.Offset
			f.Handle.RangeHint.Length = blkRange.Length
		} else if f.StreamIndex.IsSegment() {
			segStart := ctx.Ft.SegmentParam.CalcSegmentStart(f.StreamIndex.Index)
			segLen := ctx.Ft.SegmentParam.Segments[f.StreamIndex.Index]
			segEnd := segStart + segLen

			// 打开的范围不超过本段的范围

			openOff := repRange.Offset - segStart
			openOff = math2.Clamp(openOff, 0, segLen)

			openLen := segLen

			if repRange.Length != nil {
				repEnd := repRange.Offset + *repRange.Length
				openEnd := math2.Min(repEnd, segEnd)
				openLen = openEnd - openOff
			}

			f.Handle.RangeHint.Offset = openOff
			f.Handle.RangeHint.Length = &openLen
		}

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported from type %T", f)
	}
}

func buildToNode(ctx *ParseContext, t ioswitch2.To) (ops2.ToNode, error) {
	switch t := t.(type) {
	case *ioswitch2.ToShardStore:
		n := ctx.DAG.NewShardWrite(t, t.Storage.StorageID, t.FileHashStoreKey)

		if err := setEnvByAddress(n, t.Hub, t.Hub.Address); err != nil {
			return nil, err
		}

		n.Env().Pinned = true

		return n, nil

	case *ioswitch2.ToDriver:
		n := ctx.DAG.NewToDriver(t, t.Handle)
		n.Env().ToEnvDriver()
		n.Env().Pinned = true

		return n, nil

	case *ioswitch2.LoadToShared:
		n := ctx.DAG.NewSharedLoad(t, t.Storage.StorageID, t.UserID, t.PackageID, t.Path)

		if err := setEnvByAddress(n, t.Hub, t.Hub.Address); err != nil {
			return nil, err
		}

		n.Env().Pinned = true

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported to type %T", t)
	}
}

func setEnvByAddress(n dag.Node, hub cdssdk.Hub, addr cdssdk.HubAddressInfo) error {
	switch addr := addr.(type) {
	case *cdssdk.HttpAddressInfo:
		n.Env().ToEnvWorker(&ioswitch2.HttpHubWorker{Hub: hub})

	case *cdssdk.GRPCAddressInfo:
		n.Env().ToEnvWorker(&ioswitch2.AgentWorker{Hub: hub, Address: *addr})

	default:
		return fmt.Errorf("unsupported node address type %T", addr)
	}

	return nil
}

// 从SegmentJoin中删除未使用的分段
func removeUnusedSegment(ctx *ParseContext) error {
	var err error
	dag.WalkOnlyType[*ops2.SegmentJoinNode](ctx.DAG.Graph, func(node *ops2.SegmentJoinNode) bool {
		start := ctx.StreamRange.Offset
		var end *int64
		if ctx.StreamRange.Length != nil {
			e := ctx.StreamRange.Offset + *ctx.StreamRange.Length
			end = &e
		}

		segStart, segEnd := ctx.Ft.SegmentParam.CalcSegmentRange(start, end)

		node.MarkUsed(segStart, segEnd)

		for i := segStart; i < segEnd; i++ {
			if node.InputStreams().Get(i) == nil {
				err = fmt.Errorf("segment %v missed to join an raw stream", i)
				return false
			}
		}

		return true
	})

	return err
}

// 删除未使用的SegmentJoin
func removeUnusedSegmentJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ops2.SegmentJoinNode](ctx.DAG.Graph, func(node *ops2.SegmentJoinNode) bool {
		if node.Joined().To().Len() > 0 {
			return true
		}

		node.RemoveAllInputs()
		ctx.DAG.RemoveNode(node)
		return true
	})

	return changed
}

// 删除未使用的SegmentSplit
func removeUnusedSegmentSplit(ctx *ParseContext) bool {
	changed := false
	dag.WalkOnlyType[*ops2.SegmentSplitNode](ctx.DAG.Graph, func(typ *ops2.SegmentSplitNode) bool {
		// Split出来的每一个流都没有被使用，才能删除这个指令
		for _, out := range typ.OutputStreams().RawArray() {
			if out.To().Len() > 0 {
				return true
			}
		}

		typ.RemoveAllStream()
		ctx.DAG.RemoveNode(typ)
		changed = true
		return true
	})

	return changed
}

// 如果Split的结果被完全用于Join，则省略Split和Join指令
func omitSegmentSplitJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ops2.SegmentSplitNode](ctx.DAG.Graph, func(splitNode *ops2.SegmentSplitNode) bool {
		// Split指令的每一个输出都有且只有一个目的地
		var dstNode dag.Node
		for _, out := range splitNode.OutputStreams().RawArray() {
			if out.To().Len() != 1 {
				return true
			}

			if dstNode == nil {
				dstNode = out.To().Get(0)
			} else if dstNode != out.To().Get(0) {
				return true
			}
		}

		if dstNode == nil {
			return true
		}

		// 且这个目的地要是一个Join指令
		joinNode, ok := dstNode.(*ops2.SegmentJoinNode)
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
			splitInput.StreamTo(to, to.InputStreams().IndexOf(joinNode.Joined()))
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

// 删除输出流未被使用的Join指令
func removeUnusedJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ops2.ChunkedJoinNode](ctx.DAG.Graph, func(node *ops2.ChunkedJoinNode) bool {
		if node.Joined().To().Len() > 0 {
			return true
		}

		node.RemoveAllInputs()
		ctx.DAG.RemoveNode(node)
		return true
	})

	return changed
}

// 减少未使用的Multiply指令的输出流。如果减少到0，则删除该指令
func removeUnusedMultiplyOutput(ctx *ParseContext) bool {
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

		// TODO2 没有修改SlotIndex
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
func removeUnusedSplit(ctx *ParseContext) bool {
	changed := false
	dag.WalkOnlyType[*ops2.ChunkedSplitNode](ctx.DAG.Graph, func(typ *ops2.ChunkedSplitNode) bool {
		// Split出来的每一个流都没有被使用，才能删除这个指令
		for _, out := range typ.OutputStreams().RawArray() {
			if out.To().Len() > 0 {
				return true
			}
		}

		typ.RemoveAllStream()
		ctx.DAG.RemoveNode(typ)
		changed = true
		return true
	})

	return changed
}

// 如果Split的结果被完全用于Join，则省略Split和Join指令
func omitSplitJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ops2.ChunkedSplitNode](ctx.DAG.Graph, func(splitNode *ops2.ChunkedSplitNode) bool {
		// Split指令的每一个输出都有且只有一个目的地
		var dstNode dag.Node
		for _, out := range splitNode.OutputStreams().RawArray() {
			if out.To().Len() != 1 {
				return true
			}

			if dstNode == nil {
				dstNode = out.To().Get(0)
			} else if dstNode != out.To().Get(0) {
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
			splitInput.StreamTo(to, to.InputStreams().IndexOf(joinNode.Joined()))
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
func pin(ctx *ParseContext) bool {
	changed := false
	ctx.DAG.Walk(func(node dag.Node) bool {
		if node.Env().Pinned {
			return true
		}

		var toEnv *dag.NodeEnv
		for _, out := range node.OutputStreams().RawArray() {
			for _, to := range out.To().RawArray() {
				if to.Env().Type == dag.EnvUnknown {
					continue
				}

				if toEnv == nil {
					toEnv = to.Env()
				} else if !toEnv.Equals(to.Env()) {
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
			if in.From().Env().Type == dag.EnvUnknown {
				continue
			}

			if fromEnv == nil {
				fromEnv = in.From().Env()
			} else if !fromEnv.Equals(in.From().Env()) {
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

// 删除未使用的From流，不会删除FromDriver
func removeUnusedFromNode(ctx *ParseContext) {
	dag.WalkOnlyType[ops2.FromNode](ctx.DAG.Graph, func(node ops2.FromNode) bool {
		if _, ok := node.(*ops2.FromDriverNode); ok {
			return true
		}

		if node.Output().Var == nil {
			ctx.DAG.RemoveNode(node)
		}
		return true
	})
}

// 对于所有未使用的流，增加Drop指令
func dropUnused(ctx *ParseContext) {
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
func storeIPFSWriteResult(ctx *ParseContext) {
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
func generateRange(ctx *ParseContext) {
	for i := 0; i < len(ctx.Ft.Toes); i++ {
		to := ctx.Ft.Toes[i]
		toNode := ctx.ToNodes[to]

		toStrIdx := to.GetStreamIndex()
		toRng := to.GetRange()

		if toStrIdx.IsRaw() {
			n := ctx.DAG.NewRange()
			toInput := toNode.Input()
			*n.Env() = *toInput.Var.From().Env()
			rnged := n.RangeStream(toInput.Var, exec.Range{
				Offset: toRng.Offset - ctx.StreamRange.Offset,
				Length: toRng.Length,
			})
			toInput.Var.StreamNotTo(toNode, toInput.Index)
			toNode.SetInput(rnged)

		} else if toStrIdx.IsEC() {
			stripSize := int64(ctx.Ft.ECParam.ChunkSize * ctx.Ft.ECParam.K)
			blkStartIdx := ctx.StreamRange.Offset / stripSize

			blkStart := blkStartIdx * int64(ctx.Ft.ECParam.ChunkSize)

			n := ctx.DAG.NewRange()
			toInput := toNode.Input()
			*n.Env() = *toInput.Var.From().Env()
			rnged := n.RangeStream(toInput.Var, exec.Range{
				Offset: toRng.Offset - blkStart,
				Length: toRng.Length,
			})
			toInput.Var.StreamNotTo(toNode, toInput.Index)
			toNode.SetInput(rnged)
		} else if toStrIdx.IsSegment() {
			// if frNode, ok := toNode.Input().Var.From().Node.(ops2.FromNode); ok {
			// 	// 目前只有To也是分段时，才可能对接一个提供分段的From，此时不需要再生成Range指令
			// 	if frNode.GetFrom().GetStreamIndex().IsSegment() {
			// 		continue
			// 	}
			// }

			// segStart := ctx.Ft.SegmentParam.CalcSegmentStart(toStrIdx.Index)
			// strStart := segStart + toRng.Offset

			// n := ctx.DAG.NewRange()
			// toInput := toNode.Input()
			// *n.Env() = *toInput.Var.From().Node.Env()
			// rnged := n.RangeStream(toInput.Var, exec.Range{
			// 	Offset: strStart - ctx.StreamRange.Offset,
			// 	Length: toRng.Length,
			// })
			// toInput.Var.StreamNotTo(toNode, toInput.Index)
			// toNode.SetInput(rnged)
		}
	}
}

// 生成Clone指令
func generateClone(ctx *ParseContext) {
	ctx.DAG.Walk(func(node dag.Node) bool {
		for _, outVar := range node.OutputStreams().RawArray() {
			if outVar.To().Len() <= 1 {
				continue
			}

			c := ctx.DAG.NewCloneStream()
			*c.Env() = *node.Env()
			for _, to := range outVar.To().RawArray() {
				c.NewOutput().StreamTo(to, to.InputStreams().IndexOf(outVar))
			}
			outVar.To().Resize(0)
			c.SetInput(outVar)
		}

		for _, outVar := range node.OutputValues().RawArray() {
			if outVar.To().Len() <= 1 {
				continue
			}

			t := ctx.DAG.NewCloneValue()
			*t.Env() = *node.Env()
			for _, to := range outVar.To().RawArray() {
				t.NewOutput().ValueTo(to, to.InputValues().IndexOf(outVar))
			}
			outVar.To().Resize(0)
			t.SetInput(outVar)
		}

		return true
	})
}
