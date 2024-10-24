package mq

import (
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/pool"
)

type Service struct {
	taskManager    *task.Manager
	shardStorePool *pool.ShardStorePool
}

func NewService(taskMgr *task.Manager, shardStorePool *pool.ShardStorePool) *Service {
	return &Service{
		taskManager:    taskMgr,
		shardStorePool: shardStorePool,
	}
}
