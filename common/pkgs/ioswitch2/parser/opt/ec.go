package opt

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
)

// 减少未使用的Multiply指令的输出流。如果减少到0，则删除该指令
func RemoveUnusedMultiplyOutput(ctx *state.GenerateState) bool {
	changed := false
	dag.WalkOnlyType[*ops2.ECMultiplyNode](ctx.DAG.Graph, func(node *ops2.ECMultiplyNode) bool {
		outArr := node.OutputStreams().Slots.RawArray()
		for i2, out := range outArr {
			if out.Dst.Len() > 0 {
				continue
			}

			outArr[i2] = nil
			node.OutputIndexes[i2] = -2
			changed = true
		}

		node.OutputStreams().Slots.SetRawArray(lo2.RemoveAllDefault(outArr))
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
