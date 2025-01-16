package opt

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
)

// 通过流的输入输出位置来确定指令的执行位置。
// To系列的指令都会有固定的执行位置，这些位置会随着pin操作逐步扩散到整个DAG，
// 所以理论上不会出现有指令的位置始终无法确定的情况。
func Pin(ctx *state.GenerateState) bool {
	changed := false
	ctx.DAG.Walk(func(node dag.Node) bool {
		if node.Env().Pinned {
			return true
		}

		var toEnv *dag.NodeEnv
		for _, out := range node.OutputStreams().Slots.RawArray() {
			for _, to := range out.Dst.RawArray() {
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
		for _, in := range node.InputStreams().Slots.RawArray() {
			if in.Src.Env().Type == dag.EnvUnknown {
				continue
			}

			if fromEnv == nil {
				fromEnv = in.Src.Env()
			} else if !fromEnv.Equals(in.Src.Env()) {
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
