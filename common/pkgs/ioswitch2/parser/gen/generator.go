package gen

import (
	"fmt"
	"math"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

// 检查使用不同编码时参数是否设置到位
func CheckEncodingParams(ctx *state.GenerateState) error {
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
func CalcStreamRange(ctx *state.GenerateState) {
	rng := math2.NewRange(math.MaxInt64, 0)

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

func Extend(ctx *state.GenerateState) error {
	for _, fr := range ctx.Ft.Froms {
		frNode, err := buildFromNode(ctx, fr)
		if err != nil {
			return err
		}
		ctx.FromNodes[fr] = frNode

		ctx.IndexedStreams = append(ctx.IndexedStreams, state.IndexedStream{
			Stream:      frNode.Output().Var(),
			StreamIndex: fr.GetStreamIndex(),
		})

		// 对于完整文件的From，生成Split指令
		if fr.GetStreamIndex().IsRaw() {
			// 只有输入输出需要EC编码的块时，才生成相关指令
			if ctx.UseEC {
				splitNode := ctx.DAG.NewChunkedSplit(ctx.Ft.ECParam.ChunkSize, ctx.Ft.ECParam.K)
				splitNode.Split(frNode.Output().Var())
				for i := 0; i < ctx.Ft.ECParam.K; i++ {
					ctx.IndexedStreams = append(ctx.IndexedStreams, state.IndexedStream{
						Stream:      splitNode.SubStream(i),
						StreamIndex: ioswitch2.ECStream(i),
					})
				}
			}

			// 同上
			if ctx.UseSegment {
				splitNode := ctx.DAG.NewSegmentSplit(ctx.Ft.SegmentParam.Segments)
				frNode.Output().Var().ToSlot(splitNode.InputSlot())
				for i := 0; i < len(ctx.Ft.SegmentParam.Segments); i++ {
					ctx.IndexedStreams = append(ctx.IndexedStreams, state.IndexedStream{
						Stream:      splitNode.Segment(i),
						StreamIndex: ioswitch2.SegmentStream(i),
					})
				}
			}
		}
	}

	if ctx.UseEC {
		// 如果有K个不同的文件块流，则生成Multiply指令，同时针对其生成的流，生成Join指令
		ecInputStrs := make(map[int]*dag.StreamVar)
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
				ctx.IndexedStreams = append(ctx.IndexedStreams, state.IndexedStream{
					Stream:      mulNode.NewOutput(i),
					StreamIndex: ioswitch2.ECStream(i),
				})
			}

			joinNode := ctx.DAG.NewChunkedJoin(ctx.Ft.ECParam.ChunkSize)
			for i := 0; i < ctx.Ft.ECParam.K; i++ {
				// 不可能找不到流
				joinNode.AddInput(findOutputStream(ctx, ioswitch2.ECStream(i)))
			}
			ctx.IndexedStreams = append(ctx.IndexedStreams, state.IndexedStream{
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
				str.ToSlot(joinNode.InputSlot(i))
			}
		}
		ctx.IndexedStreams = append(ctx.IndexedStreams, state.IndexedStream{
			Stream:      joinNode.Joined(),
			StreamIndex: ioswitch2.RawStream(),
		})

		// SegmentJoin生成的Join指令可以用来生成EC块
		if ctx.UseEC {
			splitNode := ctx.DAG.NewChunkedSplit(ctx.Ft.ECParam.ChunkSize, ctx.Ft.ECParam.K)
			splitNode.Split(joinNode.Joined())

			mulNode := ctx.DAG.NewECMultiply(*ctx.Ft.ECParam)

			for i := 0; i < ctx.Ft.ECParam.K; i++ {
				mulNode.AddInput(splitNode.SubStream(i), i)
				ctx.IndexedStreams = append(ctx.IndexedStreams, state.IndexedStream{
					Stream:      splitNode.SubStream(i),
					StreamIndex: ioswitch2.ECStream(i),
				})
			}

			for i := 0; i < ctx.Ft.ECParam.N; i++ {
				ctx.IndexedStreams = append(ctx.IndexedStreams, state.IndexedStream{
					Stream:      mulNode.NewOutput(i),
					StreamIndex: ioswitch2.ECStream(i),
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

func buildFromNode(ctx *state.GenerateState, f ioswitch2.From) (ops2.FromNode, error) {
	var repRange math2.Range
	repRange.Offset = ctx.StreamRange.Offset
	if ctx.StreamRange.Length != nil {
		repRngLen := *ctx.StreamRange.Length
		repRange.Length = &repRngLen
	}

	var blkRange math2.Range
	if ctx.UseEC {
		blkRange.Offset = ctx.StreamRange.Offset / int64(ctx.Ft.ECParam.ChunkSize*ctx.Ft.ECParam.K) * int64(ctx.Ft.ECParam.ChunkSize)
		if ctx.StreamRange.Length != nil {
			blkRngLen := *ctx.StreamRange.Length / int64(ctx.Ft.ECParam.ChunkSize*ctx.Ft.ECParam.K) * int64(ctx.Ft.ECParam.ChunkSize)
			blkRange.Length = &blkRngLen
		}
	}

	switch f := f.(type) {
	case *ioswitch2.FromShardstore:
		t := ctx.DAG.NewShardRead(f, f.Storage.Storage.StorageID, types.NewOpen(f.FileHash))

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

func buildToNode(ctx *state.GenerateState, t ioswitch2.To) (ops2.ToNode, error) {
	switch t := t.(type) {
	case *ioswitch2.ToShardStore:
		n := ctx.DAG.NewShardWrite(t, t.Storage, t.FileHashStoreKey)

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
		n := ctx.DAG.NewSharedLoad(t, t.Storage, t.ObjectPath)

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

func findOutputStream(ctx *state.GenerateState, streamIndex ioswitch2.StreamIndex) *dag.StreamVar {
	var ret *dag.StreamVar
	for _, s := range ctx.IndexedStreams {
		if s.StreamIndex == streamIndex {
			ret = s.Stream
			break
		}
	}
	return ret
}

// 根据StreamRange，调整SegmentSplit中分段的个数和每段的大小
func FixSegmentSplit(ctx *state.GenerateState) error {
	var err error
	dag.WalkOnlyType[*ops2.SegmentSplitNode](ctx.DAG.Graph, func(node *ops2.SegmentSplitNode) bool {
		var strEnd *int64
		if ctx.StreamRange.Length != nil {
			e := ctx.StreamRange.Offset + *ctx.StreamRange.Length
			strEnd = &e
		}

		startSeg, endSeg := ctx.Ft.SegmentParam.CalcSegmentRange(ctx.StreamRange.Offset, strEnd)

		// 关闭超出范围的分段
		for i := endSeg; i < len(node.Segments); i++ {
			node.OutputStreams().Get(i).ClearAllDst()
		}
		node.OutputStreams().Slots.RemoveRange(endSeg, ctx.Ft.SegmentParam.SegmentCount()-endSeg)
		node.Segments = lo2.RemoveRange(node.Segments, endSeg, ctx.Ft.SegmentParam.SegmentCount()-endSeg)

		for i := 0; i < startSeg; i++ {
			node.OutputStreams().Get(i).ClearAllDst()
		}
		node.OutputStreams().Slots.RemoveRange(0, startSeg)
		node.Segments = lo2.RemoveRange(node.Segments, 0, startSeg)

		// StreamRange开始的位置可能在某个分段的中间，此时这个分段的大小等于流开始位置到分段结束位置的距离
		startSegStart := ctx.Ft.SegmentParam.CalcSegmentStart(startSeg)
		node.Segments[0] -= ctx.StreamRange.Offset - startSegStart

		// StreamRange结束的位置可能在某个分段的中间，此时这个分段的大小就等于流结束位置到分段起始位置的距离
		if strEnd != nil {
			endSegStart := ctx.Ft.SegmentParam.CalcSegmentStart(endSeg - 1)
			node.Segments[len(node.Segments)-1] = *strEnd - endSegStart
		}
		return true
	})

	return err
}

// 从SegmentJoin中删除未使用的分段
func FixSegmentJoin(ctx *state.GenerateState) error {
	var err error
	dag.WalkOnlyType[*ops2.SegmentJoinNode](ctx.DAG.Graph, func(node *ops2.SegmentJoinNode) bool {
		start := ctx.StreamRange.Offset
		var end *int64
		if ctx.StreamRange.Length != nil {
			e := ctx.StreamRange.Offset + *ctx.StreamRange.Length
			end = &e
		}

		startSeg, endSeg := ctx.Ft.SegmentParam.CalcSegmentRange(start, end)

		// 关闭超出范围的分段
		for i := endSeg; i < len(node.Segments); i++ {
			node.InputStreams().Get(i).NotTo(node)
		}
		node.InputStreams().Slots.RemoveRange(endSeg, ctx.Ft.SegmentParam.SegmentCount()-endSeg)
		node.Segments = lo2.RemoveRange(node.Segments, endSeg, ctx.Ft.SegmentParam.SegmentCount()-endSeg)

		for i := 0; i < startSeg; i++ {
			node.InputStreams().Get(i).NotTo(node)
		}
		node.InputStreams().Slots.RemoveRange(0, startSeg)
		node.Segments = lo2.RemoveRange(node.Segments, 0, startSeg)

		// StreamRange开始的位置可能在某个分段的中间，此时这个分段的大小等于流开始位置到分段结束位置的距离
		startSegStart := ctx.Ft.SegmentParam.CalcSegmentStart(startSeg)
		node.Segments[0] -= ctx.StreamRange.Offset - startSegStart

		// 检查一下必须的分段是否都被加入到Join中
		for i := 0; i < node.InputStreams().Len(); i++ {
			if node.InputStreams().Get(i) == nil {
				err = fmt.Errorf("segment %v missed to join an raw stream", i+startSeg)
				return false
			}
		}

		return true
	})

	return err
}
