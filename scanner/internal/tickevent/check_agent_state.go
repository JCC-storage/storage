package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

type CheckAgentState struct {
}

func NewCheckAgentState() *CheckAgentState {
	return &CheckAgentState{}
}

func (e *CheckAgentState) Execute(ctx ExecuteContext) {
	log := logger.WithType[CheckAgentState]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	hubs, err := ctx.Args.DB.Hub().GetAllHubs(ctx.Args.DB.DefCtx())
	if err != nil {
		log.Warnf("get all hubs failed, err: %s", err.Error())
		return
	}

	for _, hub := range hubs {
		ctx.Args.EventExecutor.Post(event.NewAgentCheckState(scevt.NewAgentCheckState(hub.HubID)), event.ExecuteOption{
			IsEmergency: true,
			DontMerge:   true,
		})
	}
}
