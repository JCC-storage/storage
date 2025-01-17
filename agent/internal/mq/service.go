package mq

import (
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
)

type Service struct {
	taskManager *task.Manager
	stgAgts     *agtpool.AgentPool
}

func NewService(taskMgr *task.Manager, stgAgts *agtpool.AgentPool) *Service {
	return &Service{
		taskManager: taskMgr,
		stgAgts:     stgAgts,
	}
}
