package opt

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
)

// 删除未使用的From流，不会删除FromDriver
func RemoveUnusedFromNode(ctx *state.GenerateState) {
	dag.WalkOnlyType[ops2.FromNode](ctx.DAG.Graph, func(node ops2.FromNode) bool {
		if _, ok := node.(*ops2.FromDriverNode); ok {
			return true
		}

		if node.Output().Var().Dst.Len() == 0 {
			ctx.DAG.RemoveNode(node)
		}
		return true
	})
}

// 对于所有未使用的流，增加Drop指令
func DropUnused(ctx *state.GenerateState) {
	ctx.DAG.Walk(func(node dag.Node) bool {
		for _, out := range node.OutputStreams().Slots.RawArray() {
			if out.Dst.Len() == 0 {
				n := ctx.DAG.NewDropStream()
				*n.Env() = *node.Env()
				n.SetInput(out)
			}
		}
		return true
	})
}

// 为IPFS写入指令存储结果
func StoreShardWriteResult(ctx *state.GenerateState) {
	dag.WalkOnlyType[*ops2.ShardWriteNode](ctx.DAG.Graph, func(n *ops2.ShardWriteNode) bool {
		if n.FileHashStoreKey == "" {
			return true
		}

		storeNode := ctx.DAG.NewStore()
		storeNode.Env().ToEnvDriver()

		storeNode.Store(n.FileHashStoreKey, n.FileHashVar())
		return true
	})

	dag.WalkOnlyType[*ops2.BypassToShardStoreNode](ctx.DAG.Graph, func(n *ops2.BypassToShardStoreNode) bool {
		if n.FileHashStoreKey == "" {
			return true
		}

		storeNode := ctx.DAG.NewStore()
		storeNode.Env().ToEnvDriver()

		storeNode.Store(n.FileHashStoreKey, n.FileHashVar().Var())
		return true
	})
}

// 生成Range指令。StreamRange可能超过文件总大小，但Range指令会在数据量不够时不报错而是正常返回
func GenerateRange(ctx *state.GenerateState) {
	for to, toNode := range ctx.ToNodes {
		toStrIdx := to.GetStreamIndex()
		toRng := to.GetRange()

		if toStrIdx.IsRaw() {
			n := ctx.DAG.NewRange()
			toInput := toNode.Input()
			*n.Env() = *toInput.Var().Src.Env()
			rnged := n.RangeStream(toInput.Var(), math2.Range{
				Offset: toRng.Offset - ctx.StreamRange.Offset,
				Length: toRng.Length,
			})
			toInput.Var().NotTo(toNode)
			toNode.SetInput(rnged)

		} else if toStrIdx.IsEC() {
			stripSize := int64(ctx.Ft.ECParam.ChunkSize * ctx.Ft.ECParam.K)
			blkStartIdx := ctx.StreamRange.Offset / stripSize

			blkStart := blkStartIdx * int64(ctx.Ft.ECParam.ChunkSize)

			n := ctx.DAG.NewRange()
			toInput := toNode.Input()
			*n.Env() = *toInput.Var().Src.Env()
			rnged := n.RangeStream(toInput.Var(), math2.Range{
				Offset: toRng.Offset - blkStart,
				Length: toRng.Length,
			})
			toInput.Var().NotTo(toNode)
			toNode.SetInput(rnged)
		} else if toStrIdx.IsSegment() {
			// if frNode, ok := toNode.Input().Var().From().Node.(ops2.FromNode); ok {
			// 	// 目前只有To也是分段时，才可能对接一个提供分段的From，此时不需要再生成Range指令
			// 	if frNode.GetFrom().GetStreamIndex().IsSegment() {
			// 		continue
			// 	}
			// }

			// segStart := ctx.Ft.SegmentParam.CalcSegmentStart(toStrIdx.Index)
			// strStart := segStart + toRng.Offset

			// n := ctx.DAG.NewRange()
			// toInput := toNode.Input()
			// *n.Env() = *toInput.Var().From().Node.Env()
			// rnged := n.RangeStream(toInput.Var(), exec.Range{
			// 	Offset: strStart - ctx.StreamRange.Offset,
			// 	Length: toRng.Length,
			// })
			// toInput.Var().NotTo(toNode, toInput.Index)
			// toNode.SetInput(rnged)
		}
	}
}

// 生成Clone指令
func GenerateClone(ctx *state.GenerateState) {
	ctx.DAG.Walk(func(node dag.Node) bool {
		for _, outVar := range node.OutputStreams().Slots.RawArray() {
			if outVar.Dst.Len() <= 1 {
				continue
			}

			c := ctx.DAG.NewCloneStream()
			*c.Env() = *node.Env()
			for _, dst := range outVar.Dst.RawArray() {
				c.NewOutput().To(dst, dst.InputStreams().IndexOf(outVar))
			}
			outVar.Dst.Resize(0)
			c.SetInput(outVar)
		}

		for _, outVar := range node.OutputValues().Slots.RawArray() {
			if outVar.Dst.Len() <= 1 {
				continue
			}

			t := ctx.DAG.NewCloneValue()
			*t.Env() = *node.Env()
			for _, dst := range outVar.Dst.RawArray() {
				t.NewOutput().To(dst, dst.InputValues().IndexOf(outVar))
			}
			outVar.Dst.Resize(0)
			t.SetInput(outVar)
		}

		return true
	})
}
