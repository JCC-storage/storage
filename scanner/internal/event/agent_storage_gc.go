package event

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

// AgentStorageGC 类封装了扫描器事件中的代理存储垃圾回收功能。
type AgentStorageGC struct {
	*scevt.AgentStorageGC
}

// NewAgentStorageGC 创建一个新的AgentStorageGC实例。
// evt: 传入的扫描器事件中的代理存储垃圾回收实例。
func NewAgentStorageGC(evt *scevt.AgentStorageGC) *AgentStorageGC {
	return &AgentStorageGC{
		AgentStorageGC: evt,
	}
}

// TryMerge 尝试合并两个事件。
// other: 待合并的另一个事件。
// 返回值表示是否成功合并。
func (t *AgentStorageGC) TryMerge(other Event) bool {
	event, ok := other.(*AgentStorageGC)
	if !ok {
		return false
	}

	if event.StorageID != t.StorageID {
		return false
	}

	return true
}

// Execute 执行存储垃圾回收任务。
// execCtx: 执行上下文，包含执行所需的所有参数和环境。
func (t *AgentStorageGC) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentStorageGC]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentStorageGC))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	// 尝试获取分布式锁并执行存储垃圾回收操作。

	mutex, err := reqbuilder.NewBuilder().
		// 进行垃圾回收。
		Storage().GC(t.StorageID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	// 从数据库获取存储信息和存储包信息。

	getStg, err := execCtx.Args.DB.Storage().GetByID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("getting storage: %s", err.Error())
		return
	}

	stgPkgs, err := execCtx.Args.DB.StoragePackage().GetAllByStorageID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("getting storage packages: %s", err.Error())
		return
	}

	// 创建与存储节点的代理客户端。

	agtCli, err := stgglb.AgentMQPool.Acquire(getStg.NodeID)
	if err != nil {
		log.WithField("NodeID", getStg.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	// 向代理发送存储垃圾回收请求。

	_, err = agtCli.StorageGC(agtmq.ReqStorageGC(t.StorageID, getStg.Directory, stgPkgs), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("storage gc: %s", err.Error())
		return
	}
}

// 注册消息转换器，使系统能够处理AgentStorageGC事件。
func init() {
	RegisterMessageConvertor(NewAgentStorageGC)
}
