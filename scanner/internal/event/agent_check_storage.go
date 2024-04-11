package event

import (
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

// AgentCheckStorage 代表一个用于检查存储代理的事件处理类。
type AgentCheckStorage struct {
	*scevt.AgentCheckStorage
}

// NewAgentCheckStorage 创建并返回一个初始化的 AgentCheckStorage 实例。
func NewAgentCheckStorage(evt *scevt.AgentCheckStorage) *AgentCheckStorage {
	return &AgentCheckStorage{
		AgentCheckStorage: evt,
	}
}

// TryMerge 尝试合并当前事件与另一个事件。仅当两个事件具有相同的 StorageID 时才能合并。
func (t *AgentCheckStorage) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckStorage)
	if !ok {
		return false
	}

	if t.StorageID != event.StorageID {
		return false
	}

	return true
}

// Execute 执行存储检查事件。此方法会与存储节点通信，校验存储状态并根据校验结果更新数据库。
func (t *AgentCheckStorage) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckStorage]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCheckStorage))
	defer log.Debugf("end")

	// 从数据库中获取存储和关联的节点信息
	stg, err := execCtx.Args.DB.Storage().GetByID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithField("StorageID", t.StorageID).Warnf("get storage failed, err: %s", err.Error())
		}
		return
	}

	node, err := execCtx.Args.DB.Node().GetByID(execCtx.Args.DB.SQLCtx(), stg.NodeID)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithField("StorageID", t.StorageID).Warnf("get storage node failed, err: %s", err.Error())
		}
		return
	}

	// 节点状态不正常时，直接返回
	if node.State != consts.NodeStateNormal {
		return
	}

	// 获取与存储节点通信的代理客户端
	agtCli, err := stgglb.AgentMQPool.Acquire(stg.NodeID)
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	// 向存储节点发送检查请求并处理响应
	checkResp, err := agtCli.StorageCheck(agtmq.NewStorageCheck(stg.StorageID, stg.Directory), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("checking storage: %s", err.Error())
		return
	}

	// 根据检查响应，整理出实际存在的包裹信息
	realPkgs := make(map[cdssdk.UserID]map[cdssdk.PackageID]bool)
	for _, pkg := range checkResp.Packages {
		pkgs, ok := realPkgs[pkg.UserID]
		if !ok {
			pkgs = make(map[cdssdk.PackageID]bool)
			realPkgs[pkg.UserID] = pkgs
		}

		pkgs[pkg.PackageID] = true
	}

	// 在事务中更新数据库，删除不存在的包裹信息
	execCtx.Args.DB.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		packages, err := execCtx.Args.DB.StoragePackage().GetAllByStorageID(tx, t.StorageID)
		if err != nil {
			log.Warnf("getting storage package: %s", err.Error())
			return nil
		}

		var rms []model.StoragePackage
		for _, pkg := range packages {
			pkgMap, ok := realPkgs[pkg.UserID]
			if !ok {
				rms = append(rms, pkg)
				continue
			}

			if !pkgMap[pkg.PackageID] {
				rms = append(rms, pkg)
			}
		}

		rmdPkgIDs := make(map[cdssdk.PackageID]bool)
		for _, rm := range rms {
			err := execCtx.Args.DB.StoragePackage().Delete(tx, rm.StorageID, rm.PackageID, rm.UserID)
			if err != nil {
				log.Warnf("deleting storage package: %s", err.Error())
				continue
			}
			rmdPkgIDs[rm.PackageID] = true
		}

		// 删除不再被引用的包裹
		for pkgID := range rmdPkgIDs {
			err := execCtx.Args.DB.Package().DeleteUnused(tx, pkgID)
			if err != nil {
				log.Warnf("deleting unused package: %s", err.Error())
				continue
			}
		}

		return nil
	})
}

// init 注册 AgentCheckStorage 事件处理器，使其能够响应相应的消息。
func init() {
	RegisterMessageConvertor(NewAgentCheckStorage)
}
