package event

import (
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/config"
)

type UpdatePackageAccessStatAmount struct {
	*scevt.UpdatePackageAccessStatAmount
}

func NewUpdatePackageAccessStatAmount(evt *scevt.UpdatePackageAccessStatAmount) *UpdatePackageAccessStatAmount {
	return &UpdatePackageAccessStatAmount{
		UpdatePackageAccessStatAmount: evt,
	}
}

func (t *UpdatePackageAccessStatAmount) TryMerge(other Event) bool {
	event, ok := other.(*UpdatePackageAccessStatAmount)
	if !ok {
		return false
	}

	if t.PackageIDs == nil {
		return true
	}

	if event.PackageIDs == nil {
		t.PackageIDs = nil
		return true
	}

	t.PackageIDs = append(t.PackageIDs, event.PackageIDs...)
	t.PackageIDs = lo.Uniq(t.PackageIDs)
	return true
}

func (t *UpdatePackageAccessStatAmount) Execute(execCtx ExecuteContext) {
	log := logger.WithType[UpdatePackageAccessStatAmount]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.UpdatePackageAccessStatAmount))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	if t.PackageIDs == nil {
		err := execCtx.Args.DB.PackageAccessStat().UpdateAllAmount(execCtx.Args.DB.SQLCtx(), config.Cfg().AccessStatHistoryAmount)
		if err != nil {
			log.Warnf("update all package access stat amount: %v", err)
			return
		}

		err = execCtx.Args.DB.ObjectAccessStat().UpdateAllAmount(execCtx.Args.DB.SQLCtx(), config.Cfg().AccessStatHistoryAmount)
		if err != nil {
			log.Warnf("update all object access stat amount: %v", err)
			return
		}

	} else {
		err := execCtx.Args.DB.PackageAccessStat().BatchUpdateAmount(execCtx.Args.DB.SQLCtx(), t.PackageIDs, config.Cfg().AccessStatHistoryAmount)
		if err != nil {
			log.Warnf("batch update package access stat amount: %v", err)
			return
		}

		err = execCtx.Args.DB.ObjectAccessStat().BatchUpdateAmountInPackage(execCtx.Args.DB.SQLCtx(), t.PackageIDs, config.Cfg().AccessStatHistoryAmount)
		if err != nil {
			log.Warnf("batch update object access stat amount in package: %v", err)
			return
		}
	}
}

func init() {
	RegisterMessageConvertor(NewUpdatePackageAccessStatAmount)
}
