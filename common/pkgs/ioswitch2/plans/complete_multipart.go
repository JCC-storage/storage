package plans

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func CompleteMultipart(blocks []stgmod.ObjectBlock, blockStgs []stgmod.StorageDetail, targetStg stgmod.StorageDetail, shardInfoKey string, blder *exec.PlanBuilder) error {
	da := ops2.NewGraphNodeBuilder()

	sizes := make([]int64, len(blocks))
	for i, blk := range blocks {
		sizes[i] = blk.Size
	}
	joinNode := da.NewSegmentJoin(sizes)
	joinNode.Env().ToEnvWorker(getWorkerInfo(*targetStg.MasterHub))
	joinNode.Env().Pinned = true

	for i, blk := range blocks {
		rd := da.NewShardRead(nil, blk.StorageID, types.NewOpen(blk.FileHash))
		rd.Env().ToEnvWorker(getWorkerInfo(*blockStgs[i].MasterHub))
		rd.Env().Pinned = true

		rd.Output().ToSlot(joinNode.InputSlot(i))
	}

	// TODO 应该采取更合理的方式同时支持Parser和直接生成DAG
	wr := da.NewShardWrite(nil, targetStg, shardInfoKey)
	wr.Env().ToEnvWorker(getWorkerInfo(*targetStg.MasterHub))
	wr.Env().Pinned = true

	joinNode.Joined().ToSlot(wr.Input())

	if shardInfoKey != "" {
		store := da.NewStore()
		store.Env().ToEnvDriver()
		store.Store(shardInfoKey, wr.FileHashVar())
	}

	err := plan.Compile(da.Graph, blder)
	if err != nil {
		return err
	}

	return nil
}
