package opt

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser/state"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
)

// 将SegmentJoin指令替换成分片上传指令
func UseMultipartUploadToShardStore(ctx *state.GenerateState) {
	dag.WalkOnlyType[*ops2.SegmentJoinNode](ctx.DAG.Graph, func(joinNode *ops2.SegmentJoinNode) bool {
		if joinNode.Joined().Dst.Len() != 1 {
			return true
		}

		joinDst := joinNode.Joined().Dst.Get(0)
		shardNode, ok := joinDst.(*ops2.ShardWriteNode)
		if !ok {
			return true
		}

		// SegmentJoin的输出流的范围必须与ToShardStore的输入流的范围相同，
		// 虽然可以通过调整SegmentJoin的输入流来调整范围，但太复杂，暂不支持
		toStrIdx := shardNode.GetTo().GetStreamIndex()
		toStrRng := shardNode.GetTo().GetRange()
		if toStrIdx.IsRaw() {
			if !toStrRng.Equals(ctx.StreamRange) {
				return true
			}
		} else {
			return true
		}

		// Join的目的地必须支持MultipartUpload功能才能替换成分片上传
		multiUpload, err := factory.GetBuilder(shardNode.Storage).CreateMultiparter()
		if err != nil {
			return true
		}

		// Join的每一个段的大小必须超过最小分片大小。
		// 目前只支持拆分超过最大分片的流，不支持合并多个小段流以达到最小分片大小。
		for _, size := range joinNode.Segments {
			if size < multiUpload.MinPartSize() {
				return true
			}
		}

		initNode := ctx.DAG.NewMultipartInitiator(shardNode.Storage)
		initNode.Env().CopyFrom(shardNode.Env())

		partNumber := 1
		for i, size := range joinNode.Segments {
			joinInput := joinNode.InputSlot(i)

			if size > multiUpload.MaxPartSize() {
				// 如果一个分段的大小大于最大分片大小，则需要拆分为多个小段上传
				// 拆分以及上传指令直接在流的产生节点执行
				splits := math2.SplitLessThan(size, multiUpload.MaxPartSize())
				splitNode := ctx.DAG.NewSegmentSplit(splits)
				splitNode.Env().CopyFrom(joinInput.Var().Src.Env())

				joinInput.Var().ToSlot(splitNode.InputSlot())

				for i2 := 0; i2 < len(splits); i2++ {
					uploadNode := ctx.DAG.NewMultipartUpload(shardNode.Storage, partNumber, splits[i2])
					uploadNode.Env().CopyFrom(joinInput.Var().Src.Env())

					initNode.UploadArgsVar().ToSlot(uploadNode.UploadArgsSlot())
					splitNode.SegmentVar(i2).ToSlot(uploadNode.PartStreamSlot())
					uploadNode.UploadResultVar().ToSlot(initNode.AppendPartInfoSlot())

					partNumber++
				}
			} else {
				// 否则直接上传整个分段
				uploadNode := ctx.DAG.NewMultipartUpload(shardNode.Storage, partNumber, size)
				// 上传指令直接在流的产生节点执行
				uploadNode.Env().CopyFrom(joinInput.Var().Src.Env())

				initNode.UploadArgsVar().ToSlot(uploadNode.UploadArgsSlot())
				joinInput.Var().ToSlot(uploadNode.PartStreamSlot())
				uploadNode.UploadResultVar().ToSlot(initNode.AppendPartInfoSlot())

				partNumber++
			}

			joinInput.Var().NotTo(joinNode)
		}

		bypassNode := ctx.DAG.NewBypassToShardStore(shardNode.Storage.Storage.StorageID, shardNode.FileHashStoreKey)
		bypassNode.Env().CopyFrom(shardNode.Env())

		// 分片上传Node产生的结果送到bypassNode，bypassNode将处理结果再送回分片上传Node
		initNode.BypassFileInfoVar().ToSlot(bypassNode.BypassFileInfoSlot())
		bypassNode.BypassCallbackVar().ToSlot(initNode.BypassCallbackSlot())

		// 最后删除Join指令和ToShardStore指令
		ctx.DAG.RemoveNode(joinNode)
		ctx.DAG.RemoveNode(shardNode)
		delete(ctx.ToNodes, shardNode.GetTo())
		return true
	})
}
