package event

import (
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

// AgentCheckCache 代表一个用于处理代理缓存检查事件的结构体
type AgentCheckCache struct {
	*scevt.AgentCheckCache
}

// NewAgentCheckCache 创建一个新的 AgentCheckCache 实例
func NewAgentCheckCache(evt *scevt.AgentCheckCache) *AgentCheckCache {
	return &AgentCheckCache{
		AgentCheckCache: evt,
	}
}

// TryMerge 尝试合并当前事件与另一个事件
// 如果另一个事件类型不匹配或节点ID不同，则不进行合并
func (t *AgentCheckCache) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckCache)
	if !ok {
		return false
	}

	if event.NodeID != t.NodeID {
		return false
	}

	return true
}

// Execute 执行缓存检查操作，对比本地缓存与代理返回的缓存信息，更新数据库中的缓存记录
func (t *AgentCheckCache) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckCache]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCheckCache))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	agtCli, err := stgglb.AgentMQPool.Acquire(t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	checkResp, err := agtCli.CheckCache(agtmq.NewCheckCache(), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("checking ipfs: %s", err.Error())
		return
	}

	realFileHashes := lo.SliceToMap(checkResp.FileHashes, func(hash string) (string, bool) { return hash, true })

	// 在事务中执行缓存更新操作
	execCtx.Args.DB.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		t.checkCache(execCtx, tx, realFileHashes)

		t.checkPinnedObject(execCtx, tx, realFileHashes)

		t.checkObjectBlock(execCtx, tx, realFileHashes)
		return nil
	})
}

// checkCache 对比Cache表中的记录，根据实际存在的文件哈希值，进行增加或删除操作
func (t *AgentCheckCache) checkCache(execCtx ExecuteContext, tx *sqlx.Tx, realFileHashes map[string]bool) {
	log := logger.WithType[AgentCheckCache]("Event")

	caches, err := execCtx.Args.DB.Cache().GetByNodeID(tx, t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("getting caches by node id: %s", err.Error())
		return
	}

	realFileHashesCp := make(map[string]bool)
	for k, v := range realFileHashes {
		realFileHashesCp[k] = v
	}

	var rms []string
	for _, c := range caches {
		if realFileHashesCp[c.FileHash] {
			delete(realFileHashesCp, c.FileHash)
			continue
		}
		rms = append(rms, c.FileHash)
	}

	if len(rms) > 0 {
		err = execCtx.Args.DB.Cache().NodeBatchDelete(tx, t.NodeID, rms)
		if err != nil {
			log.Warnf("batch delete node caches: %w", err.Error())
		}
	}

	if len(realFileHashesCp) > 0 {
		err = execCtx.Args.DB.Cache().BatchCreateOnSameNode(tx, lo.Keys(realFileHashesCp), t.NodeID, 0)
		if err != nil {
			log.Warnf("batch create node caches: %w", err)
			return
		}
	}
}

// checkPinnedObject 对比PinnedObject表，若实际文件不存在，则进行删除操作
func (t *AgentCheckCache) checkPinnedObject(execCtx ExecuteContext, tx *sqlx.Tx, realFileHashes map[string]bool) {
	log := logger.WithType[AgentCheckCache]("Event")

	objs, err := execCtx.Args.DB.PinnedObject().GetObjectsByNodeID(tx, t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("getting pinned objects by node id: %s", err.Error())
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
		err = execCtx.Args.DB.PinnedObject().NodeBatchDelete(tx, t.NodeID, rms)
		if err != nil {
			log.Warnf("batch delete node pinned objects: %s", err.Error())
		}
	}
}

// checkObjectBlock 对比ObjectBlock表，若实际文件不存在，则进行删除操作
func (t *AgentCheckCache) checkObjectBlock(execCtx ExecuteContext, tx *sqlx.Tx, realFileHashes map[string]bool) {
	log := logger.WithType[AgentCheckCache]("Event")

	blocks, err := execCtx.Args.DB.ObjectBlock().GetByNodeID(tx, t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("getting object blocks by node id: %s", err.Error())
		return
	}

	var rms []string
	for _, b := range blocks {
		if realFileHashes[b.FileHash] {
			continue
		}
		rms = append(rms, b.FileHash)
	}

	if len(rms) > 0 {
		err = execCtx.Args.DB.ObjectBlock().NodeBatchDelete(tx, t.NodeID, rms)
		if err != nil {
			log.Warnf("batch delete node object blocks: %s", err.Error())
		}
	}
}

// init 注册AgentCheckCache消息转换器
func init() {
	RegisterMessageConvertor(NewAgentCheckCache)
}
