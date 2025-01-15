package opt

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
)

// 删除输出流未被使用的Join指令
func RemoveUnusedJoin(ctx *state.GenerateState) bool {
	changed := false

	dag.WalkOnlyType[*ops2.ChunkedJoinNode](ctx.DAG.Graph, func(node *ops2.ChunkedJoinNode) bool {
		if node.Joined().Dst.Len() > 0 {
			return true
		}

		node.RemoveAllInputs()
		ctx.DAG.RemoveNode(node)
		return true
	})

	return changed
}

// 删除未使用的Split指令
func RemoveUnusedSplit(ctx *state.GenerateState) bool {
	changed := false
	dag.WalkOnlyType[*ops2.ChunkedSplitNode](ctx.DAG.Graph, func(typ *ops2.ChunkedSplitNode) bool {
		// Split出来的每一个流都没有被使用，才能删除这个指令
		for _, out := range typ.OutputStreams().Slots.RawArray() {
			if out.Dst.Len() > 0 {
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
func OmitSplitJoin(ctx *state.GenerateState) bool {
	changed := false

	dag.WalkOnlyType[*ops2.ChunkedSplitNode](ctx.DAG.Graph, func(splitNode *ops2.ChunkedSplitNode) bool {
		// Split指令的每一个输出都有且只有一个目的地
		var dstNode dag.Node
		for _, out := range splitNode.OutputStreams().Slots.RawArray() {
			if out.Dst.Len() != 1 {
				return true
			}

			if dstNode == nil {
				dstNode = out.Dst.Get(0)
			} else if dstNode != out.Dst.Get(0) {
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
		for _, to := range joinNode.Joined().Dst.RawArray() {
			splitInput.To(to, to.InputStreams().IndexOf(joinNode.Joined()))
		}
		splitInput.NotTo(splitNode)

		// 并删除这两个指令
		ctx.DAG.RemoveNode(joinNode)
		ctx.DAG.RemoveNode(splitNode)

		changed = true
		return true
	})

	return changed
}
