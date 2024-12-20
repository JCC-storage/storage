package tickevent

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	evt "gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

type BatchCleanPinned struct {
	lastCheckStart int
}

func NewBatchCleanPinned() *BatchCleanPinned {
	return &BatchCleanPinned{}
}

func (e *BatchCleanPinned) Execute(ctx ExecuteContext) {
	log := logger.WithType[BatchCleanPinned]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	// TODO 更好的策略
	nowHour := time.Now().Hour()
	if nowHour > 6 {
		return
	}

	packageIDs, err := ctx.Args.DB.Package().BatchGetAllPackageIDs(ctx.Args.DB.DefCtx(), e.lastCheckStart, CheckPackageBatchSize)
	if err != nil {
		log.Warnf("batch get package ids failed, err: %s", err.Error())
		return
	}

	for _, id := range packageIDs {
		ctx.Args.EventExecutor.Post(evt.NewCleanPinned(event.NewCleanPinned(id)))
	}

	// 如果结果的长度小于预期的长度，则认为已经查询了所有，下次从头再来
	if len(packageIDs) < CheckPackageBatchSize {
		e.lastCheckStart = 0
		log.Debugf("all package clean pinned, next time will start check at offset 0")

	} else {
		e.lastCheckStart += CheckPackageBatchSize
	}
}
