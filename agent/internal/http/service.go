package http

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/svcmgr"
)

type Service struct {
	swWorker *exec.Worker
	stgMgr   *svcmgr.AgentPool
}

func NewService(swWorker *exec.Worker, stgMgr *svcmgr.AgentPool) *Service {
	return &Service{
		swWorker: swWorker,
		stgMgr:   stgMgr,
	}
}
