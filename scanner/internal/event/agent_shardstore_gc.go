package event

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

// AgentShardStoreGC 类封装了扫描器事件中的AgentShardStoreGC结构。
type AgentShardStoreGC struct {
	*scevt.AgentShardStoreGC
}

// NewAgentShardStoreGC 创建一个新的AgentCacheGC实例。
// evt: 传入的扫描器事件中的AgentCacheGC实例。
func NewAgentShardStoreGC(evt *scevt.AgentShardStoreGC) *AgentShardStoreGC {
	return &AgentShardStoreGC{
		AgentShardStoreGC: evt,
	}
}

// TryMerge 尝试合并当前事件与另一个事件。
// other: 待合并的另一个事件。
// 返回值表示是否成功合并。
func (t *AgentShardStoreGC) TryMerge(other Event) bool {
	event, ok := other.(*AgentShardStoreGC)
	if !ok {
		return false
	}

	if event.StorageID != t.StorageID {
		return false
	}

	return true
}

// Execute 执行垃圾回收操作。
// execCtx: 执行上下文，包含执行所需的各种参数和环境。
func (t *AgentShardStoreGC) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentShardStoreGC]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentShardStoreGC))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	// 使用分布式锁进行资源锁定
	mutex, err := reqbuilder.NewBuilder().
		// 执行IPFS垃圾回收
		Shard().GC(t.StorageID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	// 收集需要进行垃圾回收的文件哈希值
	var allFileHashes []cdssdk.FileHash
	var masterHub cdssdk.Hub
	err = execCtx.Args.DB.DoTx(func(tx db2.SQLContext) error {
		stg, err := execCtx.Args.DB.Storage().GetByID(tx, t.StorageID)
		if err != nil {
			return fmt.Errorf("getting storage by id: %w", err)
		}

		masterHub, err = execCtx.Args.DB.Hub().GetByID(tx, stg.MasterHub)
		if err != nil {
			return fmt.Errorf("getting master hub by id: %w", err)
		}

		blocks, err := execCtx.Args.DB.ObjectBlock().GetByStorageID(tx, t.StorageID)
		if err != nil {
			return fmt.Errorf("getting object blocks by hub id: %w", err)
		}
		for _, c := range blocks {
			allFileHashes = append(allFileHashes, c.FileHash)
		}

		objs, err := execCtx.Args.DB.PinnedObject().GetObjectsByStorageID(tx, t.StorageID)
		if err != nil {
			return fmt.Errorf("getting pinned objects by hub id: %w", err)
		}
		for _, o := range objs {
			allFileHashes = append(allFileHashes, o.FileHash)
		}

		return nil
	})
	if err != nil {
		log.WithField("HubID", t.StorageID).Warn(err.Error())
		return
	}

	// 获取与节点通信的代理客户端
	agtCli, err := stgglb.AgentMQPool.Acquire(masterHub.HubID)
	if err != nil {
		log.WithField("HubID", t.StorageID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	// 向代理发送垃圾回收请求
	_, err = agtCli.CacheGC(agtmq.ReqCacheGC(t.StorageID, allFileHashes), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("HubID", t.StorageID).Warnf("ipfs gc: %s", err.Error())
		return
	}
}

// 注册消息转换器，使系统能够处理AgentCacheGC消息。
func init() {
	RegisterMessageConvertor(NewAgentShardStoreGC)
}
