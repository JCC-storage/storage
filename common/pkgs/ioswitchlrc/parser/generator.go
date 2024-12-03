package parser

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc/ops2"
)

type GenerateContext struct {
	LRC         cdssdk.LRCRedundancy
	DAG         *ops2.GraphNodeBuilder
	To          []ioswitchlrc.To
	ToNodes     map[ioswitchlrc.To]ops2.ToNode
	StreamRange exec.Range
}

// 输入一个完整文件，从这个完整文件产生任意文件块（也可再产生完整文件）。
func Encode(fr ioswitchlrc.From, toes []ioswitchlrc.To, blder *exec.PlanBuilder) error {
	if fr.GetDataIndex() != -1 {
		return fmt.Errorf("from data is not a complete file")
	}

	ctx := GenerateContext{
		LRC:     cdssdk.DefaultLRCRedundancy,
		DAG:     ops2.NewGraphNodeBuilder(),
		To:      toes,
		ToNodes: make(map[ioswitchlrc.To]ops2.ToNode),
	}

	calcStreamRange(&ctx)
	err := buildDAGEncode(&ctx, fr, toes)
	if err != nil {
		return err
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	dropUnused(&ctx)
	storeIPFSWriteResult(&ctx)
	generateClone(&ctx)
	generateRange(&ctx)

	return plan.Generate(ctx.DAG.Graph, blder)
}

func buildDAGEncode(ctx *GenerateContext, fr ioswitchlrc.From, toes []ioswitchlrc.To) error {
	frNode, err := buildFromNode(ctx, fr)
	if err != nil {
		return fmt.Errorf("building from node: %w", err)
	}

	var dataToes []ioswitchlrc.To
	var parityToes []ioswitchlrc.To

	// 先创建需要完整文件的To节点，同时统计一下需要哪些文件块
	for _, to := range toes {
		idx := to.GetDataIndex()
		if idx == -1 {
			toNode, err := buildToNode(ctx, to)
			if err != nil {
				return fmt.Errorf("building to node: %w", err)
			}
			ctx.ToNodes[to] = toNode

			toNode.SetInput(frNode.Output().Var())
		} else if idx < ctx.LRC.K {
			dataToes = append(dataToes, to)
		} else {
			parityToes = append(parityToes, to)
		}
	}

	if len(dataToes) == 0 && len(parityToes) == 0 {
		return nil
	}

	// 需要文件块，则生成Split指令
	splitNode := ctx.DAG.NewChunkedSplit(ctx.LRC.ChunkSize, ctx.LRC.K)
	splitNode.Split(frNode.Output().Var())

	for _, to := range dataToes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}
		ctx.ToNodes[to] = toNode

		toNode.SetInput(splitNode.SubStream(to.GetDataIndex()))
	}

	if len(parityToes) == 0 {
		return nil
	}

	// 需要校验块，则进一步生成Construct指令

	conType := ctx.DAG.NewLRCConstructAny(ctx.LRC)

	for i, out := range splitNode.OutputStreams().Slots.RawArray() {
		conType.AddInput(out, i)
	}

	for _, to := range parityToes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}
		ctx.ToNodes[to] = toNode

		toNode.SetInput(conType.NewOutput(to.GetDataIndex()))
	}
	return nil
}

// 提供数据块+编码块中的k个块，重建任意块，包括完整文件。
func ReconstructAny(frs []ioswitchlrc.From, toes []ioswitchlrc.To, blder *exec.PlanBuilder) error {
	ctx := GenerateContext{
		LRC:     cdssdk.DefaultLRCRedundancy,
		DAG:     ops2.NewGraphNodeBuilder(),
		To:      toes,
		ToNodes: make(map[ioswitchlrc.To]ops2.ToNode),
	}

	calcStreamRange(&ctx)
	err := buildDAGReconstructAny(&ctx, frs, toes)
	if err != nil {
		return err
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	dropUnused(&ctx)
	storeIPFSWriteResult(&ctx)
	generateClone(&ctx)
	generateRange(&ctx)

	return plan.Generate(ctx.DAG.Graph, blder)
}

func buildDAGReconstructAny(ctx *GenerateContext, frs []ioswitchlrc.From, toes []ioswitchlrc.To) error {
	frNodes := make(map[int]ops2.FromNode)
	for _, fr := range frs {
		frNode, err := buildFromNode(ctx, fr)
		if err != nil {
			return fmt.Errorf("building from node: %w", err)
		}

		frNodes[fr.GetDataIndex()] = frNode
	}

	var completeToes []ioswitchlrc.To
	var missedToes []ioswitchlrc.To

	// 先创建需要完整文件的To节点，同时统计一下需要哪些文件块
	for _, to := range toes {
		toIdx := to.GetDataIndex()
		fr := frNodes[toIdx]
		if fr != nil {
			toNode, err := buildToNode(ctx, to)
			if err != nil {
				return fmt.Errorf("building to node: %w", err)
			}
			ctx.ToNodes[to] = toNode

			toNode.SetInput(fr.Output().Var())
			continue
		}

		if toIdx == -1 {
			completeToes = append(completeToes, to)
		} else {
			missedToes = append(missedToes, to)
		}
	}

	if len(completeToes) == 0 && len(missedToes) == 0 {
		return nil
	}

	// 生成Construct指令来恢复缺少的块

	conNode := ctx.DAG.NewLRCConstructAny(ctx.LRC)
	for i, fr := range frNodes {
		conNode.AddInput(fr.Output().Var(), i)
	}

	for _, to := range missedToes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}
		ctx.ToNodes[to] = toNode

		toNode.SetInput(conNode.NewOutput(to.GetDataIndex()))
	}

	if len(completeToes) == 0 {
		return nil
	}

	// 需要完整文件，则生成Join指令

	joinNode := ctx.DAG.NewChunkedJoin(ctx.LRC.ChunkSize)

	for i := 0; i < ctx.LRC.K; i++ {
		fr := frNodes[i]
		if fr == nil {
			joinNode.AddInput(conNode.NewOutput(i))
		} else {
			joinNode.AddInput(fr.Output().Var())
		}
	}

	for _, to := range completeToes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}
		ctx.ToNodes[to] = toNode

		toNode.SetInput(joinNode.Joined())
	}

	// 如果不需要Construct任何块，则删除这个节点
	if conNode.OutputStreams().Len() == 0 {
		conNode.RemoveAllInputs()
		ctx.DAG.RemoveNode(conNode)
	}

	return nil
}

// 输入同一组的多个块，恢复出剩下缺少的一个块。
func ReconstructGroup(frs []ioswitchlrc.From, toes []ioswitchlrc.To, blder *exec.PlanBuilder) error {
	ctx := GenerateContext{
		LRC:     cdssdk.DefaultLRCRedundancy,
		DAG:     ops2.NewGraphNodeBuilder(),
		To:      toes,
		ToNodes: make(map[ioswitchlrc.To]ops2.ToNode),
	}

	calcStreamRange(&ctx)
	err := buildDAGReconstructGroup(&ctx, frs, toes)
	if err != nil {
		return err
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	dropUnused(&ctx)
	storeIPFSWriteResult(&ctx)
	generateClone(&ctx)
	generateRange(&ctx)

	return plan.Generate(ctx.DAG.Graph, blder)
}

func buildDAGReconstructGroup(ctx *GenerateContext, frs []ioswitchlrc.From, toes []ioswitchlrc.To) error {

	var inputs []*dag.StreamVar
	for _, fr := range frs {
		frNode, err := buildFromNode(ctx, fr)
		if err != nil {
			return fmt.Errorf("building from node: %w", err)
		}

		inputs = append(inputs, frNode.Output().Var())
	}

	missedGrpIdx := toes[0].GetDataIndex()
	conNode := ctx.DAG.NewLRCConstructGroup(ctx.LRC)
	missedBlk := conNode.SetupForTarget(missedGrpIdx, inputs)

	for _, to := range toes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}
		ctx.ToNodes[to] = toNode

		toNode.SetInput(missedBlk)
	}

	return nil
}
