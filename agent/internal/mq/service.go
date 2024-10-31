package mq

import (
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/mgr"
)

type Service struct {
	taskManager *task.Manager
	stgMgr      *mgr.Manager
}

func NewService(taskMgr *task.Manager, stgMgr *mgr.Manager) *Service {
	return &Service{
		taskManager: taskMgr,
		stgMgr:      stgMgr,
	}
}
