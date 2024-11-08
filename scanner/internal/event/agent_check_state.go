package event

import (
	"database/sql"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/config"
)

type AgentCheckState struct {
	*scevt.AgentCheckState
}

func NewAgentCheckState(evt *scevt.AgentCheckState) *AgentCheckState {
	return &AgentCheckState{
		AgentCheckState: evt,
	}
}

func (t *AgentCheckState) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckState)
	if !ok {
		return false
	}

	return t.HubID == event.HubID
}

func (t *AgentCheckState) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckState]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCheckState))
	defer log.Debugf("end")

	hub, err := execCtx.Args.DB.Hub().GetByID(execCtx.Args.DB.DefCtx(), t.HubID)
	if err == sql.ErrNoRows {
		return
	}

	if err != nil {
		log.WithField("HubID", t.HubID).Warnf("get hub by id failed, err: %s", err.Error())
		return
	}

	agtCli, err := stgglb.AgentMQPool.Acquire(t.HubID)
	if err != nil {
		log.WithField("HubID", t.HubID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	_, err = agtCli.GetState(agtmq.NewGetState(), mq.RequestOption{Timeout: time.Second * 30})
	if err != nil {
		log.WithField("HubID", t.HubID).Warnf("getting state: %s", err.Error())

		// 检查上次上报时间，超时的设置为不可用
		// TODO 没有上报过是否要特殊处理？
		if hub.LastReportTime != nil && time.Since(*hub.LastReportTime) > time.Duration(config.Cfg().HubUnavailableSeconds)*time.Second {
			err := execCtx.Args.DB.Hub().UpdateState(execCtx.Args.DB.DefCtx(), t.HubID, consts.HubStateUnavailable)
			if err != nil {
				log.WithField("HubID", t.HubID).Warnf("set hub state failed, err: %s", err.Error())
			}
		}
		return
	}

	// TODO 如果以后还有其他的状态，要判断哪些状态下能设置Normal
	err = execCtx.Args.DB.Hub().UpdateState(execCtx.Args.DB.DefCtx(), t.HubID, consts.HubStateNormal)
	if err != nil {
		log.WithField("HubID", t.HubID).Warnf("change hub state failed, err: %s", err.Error())
	}
}

func init() {
	RegisterMessageConvertor(NewAgentCheckState)
}
