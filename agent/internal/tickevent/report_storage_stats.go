package tickevent

import (
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
)

func ReportStorageStats(agtPool *agtpool.AgentPool, evtPub *sysevent.Publisher) {
	stgs := agtPool.GetAllAgents()
	for _, stg := range stgs {
		shard, err := stg.GetShardStore()
		if err != nil {
			continue
		}

		stats := shard.Stats()
		evtPub.Publish(&stgmod.BodyStorageStats{
			StorageID: stg.Info().Storage.StorageID,
			DataCount: stats.FileCount,
		})
	}
}
