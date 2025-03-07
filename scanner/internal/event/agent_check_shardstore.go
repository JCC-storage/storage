package event

import (
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"

	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

// AgentCheckShardStore 代表一个用于处理代理缓存检查事件的结构体
type AgentCheckShardStore struct {
	*scevt.AgentCheckShardStore
}

// NewAgentCheckShardStore 创建一个新的 AgentCheckCache 实例
func NewAgentCheckShardStore(evt *scevt.AgentCheckShardStore) *AgentCheckShardStore {
	return &AgentCheckShardStore{
		AgentCheckShardStore: evt,
	}
}

// TryMerge 尝试合并当前事件与另一个事件
// 如果另一个事件类型不匹配或节点ID不同，则不进行合并
func (t *AgentCheckShardStore) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckShardStore)
	if !ok {
		return false
	}

	if event.StorageID != t.StorageID {
		return false
	}

	return true
}

// Execute 执行缓存检查操作，对比本地缓存与代理返回的缓存信息，更新数据库中的缓存记录
func (t *AgentCheckShardStore) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckShardStore]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCheckShardStore))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	stg, err := execCtx.Args.DB.Storage().GetByID(execCtx.Args.DB.DefCtx(), t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("getting shard storage by storage id: %s", err.Error())
		return
	}

	agtCli, err := stgglb.AgentMQPool.Acquire(stg.MasterHub)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	checkResp, err := agtCli.CheckCache(agtmq.NewCheckCache(t.StorageID), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("checking shard store: %s", err.Error())
		return
	}

	realFileHashes := lo.SliceToMap(checkResp.FileHashes, func(hash cdssdk.FileHash) (cdssdk.FileHash, bool) { return hash, true })

	// 在事务中执行缓存更新操作
	execCtx.Args.DB.DoTx(func(tx db2.SQLContext) error {
		t.checkCache(execCtx, tx, realFileHashes)

		t.checkPinnedObject(execCtx, tx, realFileHashes)

		t.checkObjectBlock(execCtx, tx, realFileHashes)
		return nil
	})
}

// checkCache 对比Cache表中的记录，根据实际存在的文件哈希值，进行增加或删除操作
func (t *AgentCheckShardStore) checkCache(execCtx ExecuteContext, tx db2.SQLContext, realFileHashes map[cdssdk.FileHash]bool) {
	log := logger.WithType[AgentCheckShardStore]("Event")

	caches, err := execCtx.Args.DB.Cache().GetByStorageID(tx, t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("getting caches by storage id: %s", err.Error())
		return
	}

	realFileHashesCp := make(map[cdssdk.FileHash]bool)
	for k, v := range realFileHashes {
		realFileHashesCp[k] = v
	}

	var rms []cdssdk.FileHash
	for _, c := range caches {
		if realFileHashesCp[c.FileHash] {
			delete(realFileHashesCp, c.FileHash)
			continue
		}
		rms = append(rms, c.FileHash)
	}

	if len(rms) > 0 {
		err = execCtx.Args.DB.Cache().StorageBatchDelete(tx, t.StorageID, rms)
		if err != nil {
			log.Warnf("batch delete storage caches: %w", err.Error())
		}
	}

	if len(realFileHashesCp) > 0 {
		err = execCtx.Args.DB.Cache().BatchCreateOnSameStorage(tx, lo.Keys(realFileHashesCp), t.StorageID, 0)
		if err != nil {
			log.Warnf("batch create storage caches: %w", err)
			return
		}
	}
}

// checkPinnedObject 对比PinnedObject表，若实际文件不存在，则进行删除操作
func (t *AgentCheckShardStore) checkPinnedObject(execCtx ExecuteContext, tx db2.SQLContext, realFileHashes map[cdssdk.FileHash]bool) {
	log := logger.WithType[AgentCheckShardStore]("Event")

	objs, err := execCtx.Args.DB.PinnedObject().GetObjectsByStorageID(tx, t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("getting pinned objects by storage id: %s", err.Error())
		return
	}

	var rms []cdssdk.ObjectID
	for _, c := range objs {
		if realFileHashes[c.FileHash] {
			continue
		}
		rms = append(rms, c.ObjectID)
	}

	if len(rms) > 0 {
		err = execCtx.Args.DB.PinnedObject().StorageBatchDelete(tx, t.StorageID, rms)
		if err != nil {
			log.Warnf("batch delete storage pinned objects: %s", err.Error())
		}
	}
}

// checkObjectBlock 对比ObjectBlock表，若实际文件不存在，则进行删除操作
func (t *AgentCheckShardStore) checkObjectBlock(execCtx ExecuteContext, tx db2.SQLContext, realFileHashes map[cdssdk.FileHash]bool) {
	log := logger.WithType[AgentCheckShardStore]("Event")

	blocks, err := execCtx.Args.DB.ObjectBlock().GetByStorageID(tx, t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("getting object blocks by storage id: %s", err.Error())
		return
	}

	var rms []cdssdk.FileHash
	for _, b := range blocks {
		if realFileHashes[b.FileHash] {
			continue
		}
		rms = append(rms, b.FileHash)
	}

	if len(rms) > 0 {
		err = execCtx.Args.DB.ObjectBlock().StorageBatchDelete(tx, t.StorageID, rms)
		if err != nil {
			log.Warnf("batch delete storage object blocks: %s", err.Error())
		}
	}
}

// init 注册AgentCheckCache消息转换器
func init() {
	RegisterMessageConvertor(NewAgentCheckShardStore)
}
