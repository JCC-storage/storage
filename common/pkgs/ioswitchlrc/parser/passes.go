package parser

import (
	"fmt"
	"math"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

// 计算输入流的打开范围。会把流的范围按条带大小取整
func calcStreamRange(ctx *GenerateContext) {
	stripSize := int64(ctx.LRC.ChunkSize * ctx.LRC.K)

	rng := exec.Range{
		Offset: math.MaxInt64,
	}

	for _, to := range ctx.To {
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

			blkStartIndex := math2.FloorDiv(toRng.Offset, int64(ctx.LRC.ChunkSize))
			rng.ExtendStart(blkStartIndex * stripSize)
			if toRng.Length != nil {
				blkEndIndex := math2.CeilDiv(toRng.Offset+*toRng.Length, int64(ctx.LRC.ChunkSize))
				rng.ExtendEnd(blkEndIndex * stripSize)
			} else {
				rng.Length = nil
			}
		}
	}

	ctx.StreamRange = rng
}

func buildFromNode(ctx *GenerateContext, f ioswitchlrc.From) (ops2.FromNode, error) {
	var repRange exec.Range
	var blkRange exec.Range

	repRange.Offset = ctx.StreamRange.Offset
	blkRange.Offset = ctx.StreamRange.Offset / int64(ctx.LRC.ChunkSize*ctx.LRC.K) * int64(ctx.LRC.ChunkSize)
	if ctx.StreamRange.Length != nil {
		repRngLen := *ctx.StreamRange.Length
		repRange.Length = &repRngLen

		blkRngLen := *ctx.StreamRange.Length / int64(ctx.LRC.ChunkSize*ctx.LRC.K) * int64(ctx.LRC.ChunkSize)
		blkRange.Length = &blkRngLen
	}

	switch f := f.(type) {
	case *ioswitchlrc.FromNode:
		t := ctx.DAG.NewShardRead(f.Storage.StorageID, types.NewOpen(f.FileHash))

		if f.DataIndex == -1 {
			t.Open.WithNullableLength(repRange.Offset, repRange.Length)
		} else {
			t.Open.WithNullableLength(blkRange.Offset, blkRange.Length)
		}

		// TODO2 支持HTTP协议
		t.Env().ToEnvWorker(&ioswitchlrc.AgentWorker{Hub: f.Hub, Address: *f.Hub.Address.(*cdssdk.GRPCAddressInfo)})
		t.Env().Pinned = true

		return t, nil

	case *ioswitchlrc.FromDriver:
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

func buildToNode(ctx *GenerateContext, t ioswitchlrc.To) (ops2.ToNode, error) {
	switch t := t.(type) {
	case *ioswitchlrc.ToNode:
		n := ctx.DAG.NewShardWrite(t.FileHashStoreKey)
		switch addr := t.Hub.Address.(type) {
		// case *cdssdk.HttpAddressInfo:
		// n.Env().ToEnvWorker(&ioswitchlrc.HttpHubWorker{Node: t.Hub})
		// TODO2 支持HTTP协议
		case *cdssdk.GRPCAddressInfo:
			n.Env().ToEnvWorker(&ioswitchlrc.AgentWorker{Hub: t.Hub, Address: *addr})

		default:
			return nil, fmt.Errorf("unsupported node address type %T", addr)
		}

		n.Env().Pinned = true

		return n, nil

	case *ioswitchlrc.ToDriver:
		n := ctx.DAG.NewToDriver(t.Handle)
		n.Env().ToEnvDriver()
		n.Env().Pinned = true

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported to type %T", t)
	}
}

// 通过流的输入输出位置来确定指令的执行位置。
// To系列的指令都会有固定的执行位置，这些位置会随着pin操作逐步扩散到整个DAG，
// 所以理论上不会出现有指令的位置始终无法确定的情况。
func pin(ctx *GenerateContext) bool {
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

// 对于所有未使用的流，增加Drop指令
func dropUnused(ctx *GenerateContext) {
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
func storeIPFSWriteResult(ctx *GenerateContext) {
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
func generateRange(ctx *GenerateContext) {
	for i := 0; i < len(ctx.To); i++ {
		to := ctx.To[i]
		toNode := ctx.ToNodes[to]

		toDataIdx := to.GetDataIndex()
		toRng := to.GetRange()

		if toDataIdx == -1 {
			n := ctx.DAG.NewRange()
			toInput := toNode.Input()
			*n.Env() = *toInput.Var.From().Env()
			rnged := n.RangeStream(toInput.Var, exec.Range{
				Offset: toRng.Offset - ctx.StreamRange.Offset,
				Length: toRng.Length,
			})
			toInput.Var.StreamNotTo(toNode, toInput.Index)
			toNode.SetInput(rnged)

		} else {
			stripSize := int64(ctx.LRC.ChunkSize * ctx.LRC.K)
			blkStartIdx := ctx.StreamRange.Offset / stripSize

			blkStart := blkStartIdx * int64(ctx.LRC.ChunkSize)

			n := ctx.DAG.NewRange()
			toInput := toNode.Input()
			*n.Env() = *toInput.Var.From().Env()
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
func generateClone(ctx *GenerateContext) {
	ctx.DAG.Walk(func(node dag.Node) bool {
		for _, outVar := range node.OutputStreams().RawArray() {
			if outVar.To().Len() <= 1 {
				continue
			}

			t := ctx.DAG.NewCloneStream()
			*t.Env() = *node.Env()
			for _, to := range outVar.To().RawArray() {
				t.NewOutput().StreamTo(to, to.InputStreams().IndexOf(outVar))
			}
			outVar.To().Resize(0)
			t.SetInput(outVar)
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
