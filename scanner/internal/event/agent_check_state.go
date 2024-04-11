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

// AgentCheckState 类封装了扫描器代理检查状态的事件。
type AgentCheckState struct {
	*scevt.AgentCheckState
}

// NewAgentCheckState 创建一个新的AgentCheckState实例。
// evt: 传入的AgentCheckState实例。
// 返回: 新创建的AgentCheckState指针。
func NewAgentCheckState(evt *scevt.AgentCheckState) *AgentCheckState {
	return &AgentCheckState{
		AgentCheckState: evt,
	}
}

// TryMerge 尝试合并当前事件与另一个事件。
// other: 待合并的另一个事件。
// 返回: 成功合并返回true，否则返回false。
func (t *AgentCheckState) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckState)
	if !ok {
		return false
	}

	return t.NodeID == event.NodeID
}

// Execute 执行节点状态检查操作。
// execCtx: 执行上下文，包含执行时所需的所有参数和环境。
func (t *AgentCheckState) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckState]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCheckState))
	defer log.Debugf("end")

	// 尝试根据节点ID获取节点信息
	node, err := execCtx.Args.DB.Node().GetByID(execCtx.Args.DB.SQLCtx(), t.NodeID)
	if err == sql.ErrNoRows {
		return
	}

	// 获取节点失败的处理
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("get node by id failed, err: %s", err.Error())
		return
	}

	// 获取代理客户端
	agtCli, err := stgglb.AgentMQPool.Acquire(t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	// 向代理请求获取当前状态
	getResp, err := agtCli.GetState(agtmq.NewGetState(), mq.RequestOption{Timeout: time.Second * 30})
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("getting state: %s", err.Error())

		// 检查节点上次上报时间，若超时则设置节点为不可用状态
		if node.LastReportTime != nil && time.Since(*node.LastReportTime) > time.Duration(config.Cfg().NodeUnavailableSeconds)*time.Second {
			err := execCtx.Args.DB.Node().UpdateState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NodeStateUnavailable)
			if err != nil {
				log.WithField("NodeID", t.NodeID).Warnf("set node state failed, err: %s", err.Error())
			}
		}
		return
	}

	// 根据代理返回的节点状态更新节点状态
	if getResp.IPFSState != consts.IPFSStateOK {
		log.WithField("NodeID", t.NodeID).Warnf("IPFS status is %s, set node state unavailable", getResp.IPFSState)

		err := execCtx.Args.DB.Node().UpdateState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NodeStateUnavailable)
		if err != nil {
			log.WithField("NodeID", t.NodeID).Warnf("change node state failed, err: %s", err.Error())
		}
		return
	}

	// 更新节点状态为正常
	err = execCtx.Args.DB.Node().UpdateState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NodeStateNormal)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("change node state failed, err: %s", err.Error())
	}
}

// init 注册AgentCheckState消息转换器。
func init() {
	RegisterMessageConvertor(NewAgentCheckState)
}
