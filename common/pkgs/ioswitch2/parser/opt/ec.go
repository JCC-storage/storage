package opt

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
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

// 替换ECMultiply指令
func UseECMultiplier(ctx *state.GenerateState) {
	dag.WalkOnlyType[*ops2.ECMultiplyNode](ctx.DAG.Graph, func(mulNode *ops2.ECMultiplyNode) bool {
		if mulNode.OutputStreams().Len() == 0 {
			return true
		}

		// 暂不支持编解码流的一部分
		if ctx.StreamRange.Offset != 0 || ctx.StreamRange.Length != nil {
			return true
		}

		var to *ioswitch2.ToShardStore
		var swNodes []*ops2.ShardWriteNode
		// 所有的输出流必须有且只有一个相同的目的地
		// 暂时只支持分片存储
		for i := 0; i < mulNode.OutputStreams().Len(); i++ {
			if mulNode.OutputStreams().Get(i).Dst.Len() != 1 {
				return true
			}

			dstNode := mulNode.OutputStreams().Get(i).Dst.Get(0)
			swNode, ok := dstNode.(*ops2.ShardWriteNode)
			if !ok {
				return true
			}

			if to == nil {
				to = swNode.To
			} else if to.Storage.Storage.StorageID != swNode.Storage.Storage.StorageID {
				return true
			}
			swNodes = append(swNodes, swNode)
		}
		_, err := factory.GetBuilder(to.Storage).CreateECMultiplier()
		if err != nil {
			return true
		}

		// 每一个输入流都必须直接来自于存储服务，而且要支持通过HTTP读取文件
		var srNodes []*ops2.ShardReadNode
		for i := 0; i < mulNode.InputStreams().Len(); i++ {
			inNode := mulNode.InputStreams().Get(i).Src
			srNode, ok := inNode.(*ops2.ShardReadNode)
			if !ok {
				return true
			}

			if !factory.GetBuilder(srNode.From.Storage).ShardStoreDesc().HasBypassHTTPRead() {
				return true
			}

			srNodes = append(srNodes, srNode)
		}

		// 检查满足条件后，替换ECMultiply指令
		callMul := ctx.DAG.NewCallECMultiplier(to.Storage)
		switch addr := to.Hub.Address.(type) {
		case *cdssdk.HttpAddressInfo:
			callMul.Env().ToEnvWorker(&ioswitch2.HttpHubWorker{Hub: to.Hub})
			callMul.Env().Pinned = true

		case *cdssdk.GRPCAddressInfo:
			callMul.Env().ToEnvWorker(&ioswitch2.AgentWorker{Hub: to.Hub, Address: *addr})
			callMul.Env().Pinned = true

		default:
			return true
		}

		callMul.InitFrom(mulNode)
		for i, srNode := range srNodes {
			srNode.Output().Var().NotTo(mulNode)
			// 只有完全没有输出的ShardReadNode才可以被删除
			if srNode.Output().Var().Dst.Len() == 0 {
				ctx.DAG.RemoveNode(srNode)
				delete(ctx.FromNodes, srNode.From)
			}

			hbr := ctx.DAG.NewBypassFromShardStoreHTTP(srNode.StorageID, srNode.From.FileHash)
			hbr.Env().CopyFrom(srNode.Env())
			hbr.HTTPRequestVar().ToSlot(callMul.InputSlot(i))
		}

		for i, swNode := range swNodes {
			ctx.DAG.RemoveNode(swNode)
			delete(ctx.ToNodes, swNode.To)

			bs := ctx.DAG.NewBypassToShardStore(to.Storage.Storage.StorageID, swNode.FileHashStoreKey)
			bs.Env().CopyFrom(swNode.Env())

			callMul.OutputVar(i).ToSlot(bs.BypassFileInfoSlot())
			bs.BypassCallbackVar().ToSlot(callMul.BypassCallbackSlot(i))
		}

		ctx.DAG.RemoveNode(mulNode)
		return true
	})
}
