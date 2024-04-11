package event

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

// AgentCacheGC 类封装了扫描器事件中的AgentCacheGC结构。
type AgentCacheGC struct {
	*scevt.AgentCacheGC
}

// NewAgentCacheGC 创建一个新的AgentCacheGC实例。
// evt: 传入的扫描器事件中的AgentCacheGC实例。
func NewAgentCacheGC(evt *scevt.AgentCacheGC) *AgentCacheGC {
	return &AgentCacheGC{
		AgentCacheGC: evt,
	}
}

// TryMerge 尝试合并当前事件与另一个事件。
// other: 待合并的另一个事件。
// 返回值表示是否成功合并。
func (t *AgentCacheGC) TryMerge(other Event) bool {
	event, ok := other.(*AgentCacheGC)
	if !ok {
		return false
	}

	if event.NodeID != t.NodeID {
		return false
	}

	return true
}

// Execute 执行垃圾回收操作。
// execCtx: 执行上下文，包含执行所需的各种参数和环境。
func (t *AgentCacheGC) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCacheGC]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCacheGC))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	// 使用分布式锁进行资源锁定
	mutex, err := reqbuilder.NewBuilder().
		// 执行IPFS垃圾回收
		IPFS().GC(t.NodeID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	// 收集需要进行垃圾回收的文件哈希值
	var allFileHashes []string
	err = execCtx.Args.DB.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		blocks, err := execCtx.Args.DB.ObjectBlock().GetByNodeID(tx, t.NodeID)
		if err != nil {
			return fmt.Errorf("getting object blocks by node id: %w", err)
		}
		for _, c := range blocks {
			allFileHashes = append(allFileHashes, c.FileHash)
		}

		objs, err := execCtx.Args.DB.PinnedObject().GetObjectsByNodeID(tx, t.NodeID)
		if err != nil {
			return fmt.Errorf("getting pinned objects by node id: %w", err)
		}
		for _, o := range objs {
			allFileHashes = append(allFileHashes, o.FileHash)
		}

		return nil
	})
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warn(err.Error())
		return
	}

	// 获取与节点通信的代理客户端
	agtCli, err := stgglb.AgentMQPool.Acquire(t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	// 向代理发送垃圾回收请求
	_, err = agtCli.CacheGC(agtmq.ReqCacheGC(allFileHashes), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("ipfs gc: %s", err.Error())
		return
	}
}

// 注册消息转换器，使系统能够处理AgentCacheGC消息。
func init() {
	RegisterMessageConvertor(NewAgentCacheGC)
}
