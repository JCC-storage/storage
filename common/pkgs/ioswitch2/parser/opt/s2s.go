package opt

import (
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
)

// 将直接从一个存储服务传到另一个存储服务的过程换成S2S传输
func UseS2STransfer(ctx *state.GenerateState) {
	// S2S传输暂不支持只传文件的一部分
	if ctx.StreamRange.Offset != 0 || ctx.StreamRange.Length != nil {
		return
	}

	for fr, frNode := range ctx.FromNodes {
		fromShard, ok := fr.(*ioswitch2.FromShardstore)
		if !ok {
			continue
		}

		fromStgBld := factory.GetBuilder(fromShard.Storage)
		if !fromStgBld.ShardStoreDesc().HasBypassRead() {
			continue
		}

		s2s, err := fromStgBld.CreateS2STransfer()
		if err != nil {
			continue
		}

		// 此输出流的所有目的地都要能支持S2S传输
		outVar := frNode.Output().Var()
		if outVar.Dst.Len() == 0 {
			continue
		}

		failed := false
		var toShards []*ops2.ShardWriteNode
		// var toShareds []*ops2.SharedLoadNode

	loop:
		for i := 0; i < outVar.Dst.Len(); i++ {
			dstNode := outVar.Dst.Get(i)

			switch dstNode := dstNode.(type) {
			case *ops2.ShardWriteNode:
				dstStgBld := factory.GetBuilder(dstNode.Storage)
				if !dstStgBld.ShardStoreDesc().HasBypassWrite() {
					failed = true
					break
				}

				if !s2s.CanTransfer(dstNode.Storage) {
					failed = true
					break
				}

				toShards = append(toShards, dstNode)

				/* TODO 暂不支持共享存储服务
				case *ops2.SharedLoadNode:
					if !s2s.CanTransfer(to.Storage) {
						failed = true
						break
					}
					toShareds = append(toShareds, to)
				*/
			default:
				failed = true
				break loop
			}
		}
		if failed {
			continue
		}

		for _, toShard := range toShards {
			s2sNode := ctx.DAG.NewS2STransfer(fromShard.Storage, toShard.Storage)
			// 直传指令在目的地Hub上执行
			s2sNode.Env().CopyFrom(toShard.Env())

			// 先获取文件路径，送到S2S节点
			brNode := ctx.DAG.NewBypassFromShardStore(fromShard.Storage.Storage.StorageID, fromShard.FileHash)
			brNode.Env().CopyFrom(frNode.Env())
			brNode.FilePathVar().ToSlot(s2sNode.SrcPathSlot())

			// 传输结果通知目的节点
			bwNode := ctx.DAG.NewBypassToShardStore(toShard.Storage.Storage.StorageID, toShard.To.FileHashStoreKey)
			bwNode.Env().CopyFrom(toShard.Env())

			s2sNode.BypassFileInfoVar().ToSlot(bwNode.BypassFileInfoSlot())
			bwNode.BypassCallbackVar().ToSlot(s2sNode.BypassCallbackSlot())

			// 从计划中删除目标节点
			ctx.DAG.RemoveNode(toShard)
			delete(ctx.ToNodes, toShard.To)
		}

		/*
			for _, toShared := range toShareds {
				s2sNode := ctx.DAG.NewS2STransfer(fromShard.Storage, toShared.Storage)
				// 直传指令在目的地Hub上执行
				s2sNode.Env().CopyFrom(toShared.Env())

				// 先获取文件路径，送到S2S节点
				brNode := ctx.DAG.NewBypassFromShardStore(fromShard.Storage.Storage.StorageID, fromShard.FileHash)
				brNode.Env().CopyFrom(toShared.Env())
				brNode.FilePathVar().ToSlot(s2sNode.SrcPathSlot())

				// 传输结果通知目的节点
				to := toShared.To.(*ioswitch2.LoadToShared)
				bwNode := ctx.DAG.NewBypassToShardStore(toShard.Storage.Storage.StorageID, to.FileHashStoreKey)
				bwNode.Env().CopyFrom(toShard.Env())

				s2sNode.BypassFileInfoVar().ToSlot(bwNode.BypassFileInfoSlot())
				bwNode.BypassCallbackVar().ToSlot(s2sNode.BypassCallbackSlot())

				// 从计划中删除目标节点
				ctx.DAG.RemoveNode(toShared)
				delete(ctx.ToNodes, toShared.To)
			}
		*/

		// 从计划中删除源节点
		ctx.DAG.RemoveNode(frNode)
		delete(ctx.FromNodes, fr)
	}
}
