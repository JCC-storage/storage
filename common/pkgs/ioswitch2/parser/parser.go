package parser

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/gen"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/opt"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
)

func Parse(ft ioswitch2.FromTo, blder *exec.PlanBuilder) error {
	state := state.InitGenerateState(ft)

	// 分成两个阶段：
	// 1. 基于From和To生成更多指令，初步匹配to的需求

	err := gen.CheckEncodingParams(state)
	if err != nil {
		return err
	}

	// 计算一下打开流的范围
	gen.CalcStreamRange(state)

	err = gen.Extend(state)
	if err != nil {
		return err
	}

	// 2. 优化上一步生成的指令

	err = gen.FixSegmentJoin(state)
	if err != nil {
		return err
	}

	err = gen.FixSegmentSplit(state)
	if err != nil {
		return err
	}

	// 对于删除指令的优化，需要反复进行，直到没有变化为止。
	// 从目前实现上来说不会死循环
	for {
		opted := false
		if opt.RemoveUnusedJoin(state) {
			opted = true
		}
		if opt.RemoveUnusedMultiplyOutput(state) {
			opted = true
		}
		if opt.RemoveUnusedSplit(state) {
			opted = true
		}
		if opt.OmitSplitJoin(state) {
			opted = true
		}
		if opt.RemoveUnusedSegmentJoin(state) {
			opted = true
		}
		if opt.RemoveUnusedSegmentSplit(state) {
			opted = true
		}
		if opt.OmitSegmentSplitJoin(state) {
			opted = true
		}

		if !opted {
			break
		}
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for opt.Pin(state) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	opt.RemoveUnusedFromNode(state)
	opt.UseS2STransfer(state)
	opt.UseMultipartUploadToShardStore(state)
	opt.DropUnused(state)
	opt.StoreShardWriteResult(state)
	opt.GenerateRange(state)
	opt.GenerateClone(state)

	return plan.Compile(state.DAG.Graph, blder)
}
