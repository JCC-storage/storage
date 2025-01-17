package tickevent

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

type StorageGC struct {
	storageIDs []cdssdk.StorageID
}

func NewStorageGC() *StorageGC {
	return &StorageGC{}
}

func (e *StorageGC) Execute(ctx ExecuteContext) {
	log := logger.WithType[StorageGC]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	if len(e.storageIDs) == 0 {
		// 0点开始检查
		if time.Now().Hour() > 0 {
			return
		}

		stgIDs, err := ctx.Args.DB.Storage().GetAllIDs(ctx.Args.DB.DefCtx())
		if err != nil {
			log.Warnf("get all storage ids: %v", err)
			return
		}
		e.storageIDs = stgIDs
	}

	if len(e.storageIDs) == 0 {
		return
	}

	ctx.Args.EventExecutor.Post(event.NewAgentShardStoreGC(scevt.NewAgentShardStoreGC(e.storageIDs[0])))
	e.storageIDs = e.storageIDs[1:]
}
