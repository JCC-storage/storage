package opt

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
)

// 删除未使用的SegmentJoin
func RemoveUnusedSegmentJoin(ctx *state.GenerateState) bool {
	changed := false

	dag.WalkOnlyType[*ops2.SegmentJoinNode](ctx.DAG.Graph, func(node *ops2.SegmentJoinNode) bool {
		if node.Joined().Dst.Len() > 0 {
			return true
		}

		node.RemoveAllInputs()
		ctx.DAG.RemoveNode(node)
		return true
	})

	return changed
}

// 删除未使用的SegmentSplit
func RemoveUnusedSegmentSplit(ctx *state.GenerateState) bool {
	changed := false
	dag.WalkOnlyType[*ops2.SegmentSplitNode](ctx.DAG.Graph, func(typ *ops2.SegmentSplitNode) bool {
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
func OmitSegmentSplitJoin(ctx *state.GenerateState) bool {
	changed := false

	dag.WalkOnlyType[*ops2.SegmentSplitNode](ctx.DAG.Graph, func(splitNode *ops2.SegmentSplitNode) bool {
		// 随便找一个输出流的目的地
		splitOut := splitNode.OutputStreams().Get(0)
		if splitOut.Dst.Len() != 1 {
			return true
		}
		dstNode := splitOut.Dst.Get(0)

		// 这个目的地要是一个Join指令
		joinNode, ok := dstNode.(*ops2.SegmentJoinNode)
		if !ok {
			return true
		}

		if splitNode.OutputStreams().Len() != joinNode.Joined().Dst.Len() {
			return true
		}

		// Join指令的输入必须全部来自Split指令的输出，且位置要相同
		for i := 0; i < splitNode.OutputStreams().Len(); i++ {
			splitOut := splitNode.OutputStreams().Get(i)
			joinIn := joinNode.InputStreams().Get(i)
			if splitOut != joinIn {
				return true
			}

			if splitOut != nil && splitOut.Dst.Len() != 1 {
				return true
			}
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
