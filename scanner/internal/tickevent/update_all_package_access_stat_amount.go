package tickevent

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	evt "gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

type UpdateAllPackageAccessStatAmount struct {
	todayUpdated bool
}

func NewUpdateAllPackageAccessStatAmount() *UpdateAllPackageAccessStatAmount {
	return &UpdateAllPackageAccessStatAmount{}
}

func (e *UpdateAllPackageAccessStatAmount) Execute(ctx ExecuteContext) {
	log := logger.WithType[UpdateAllPackageAccessStatAmount]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	nowHour := time.Now().Hour()
	if nowHour != 0 {
		e.todayUpdated = false
		return
	}
	if e.todayUpdated {
		return
	}
	e.todayUpdated = true

	ctx.Args.EventExecutor.Post(evt.NewUpdatePackageAccessStatAmount(event.NewUpdatePackageAccessStatAmount(nil)))
}
