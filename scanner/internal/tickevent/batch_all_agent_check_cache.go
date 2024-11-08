package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

const AGENT_CHECK_CACHE_BATCH_SIZE = 2

type BatchAllAgentCheckCache struct {
	stgIDs []cdssdk.StorageID
}

func NewBatchAllAgentCheckCache() *BatchAllAgentCheckCache {
	return &BatchAllAgentCheckCache{}
}

func (e *BatchAllAgentCheckCache) Execute(ctx ExecuteContext) {
	log := logger.WithType[BatchAllAgentCheckCache]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	if e.stgIDs == nil || len(e.stgIDs) == 0 {
		ids, err := ctx.Args.DB.Storage().GetAllIDs(ctx.Args.DB.DefCtx())
		if err != nil {
			log.Warnf("get all storages failed, err: %s", err.Error())
			return
		}

		log.Debugf("new check start, get all storages")
		e.stgIDs = ids
	}

	checkedCnt := 0
	for ; checkedCnt < len(e.stgIDs) && checkedCnt < AGENT_CHECK_CACHE_BATCH_SIZE; checkedCnt++ {
		// nil代表进行全量检查
		ctx.Args.EventExecutor.Post(event.NewAgentCheckCache(scevt.NewAgentCheckCache(e.stgIDs[checkedCnt])))
	}
	e.stgIDs = e.stgIDs[checkedCnt:]
}
