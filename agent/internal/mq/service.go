package mq

import (
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/svcmgr"
)

type Service struct {
	taskManager *task.Manager
	stgMgr      *svcmgr.Manager
}

func NewService(taskMgr *task.Manager, stgMgr *svcmgr.Manager) *Service {
	return &Service{
		taskManager: taskMgr,
		stgMgr:      stgMgr,
	}
}
