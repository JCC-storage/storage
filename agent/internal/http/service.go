package http

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/svcmgr"
)

type Service struct {
	swWorker *exec.Worker
	stgMgr   *svcmgr.Manager
}

func NewService(swWorker *exec.Worker, stgMgr *svcmgr.Manager) *Service {
	return &Service{
		swWorker: swWorker,
		stgMgr:   stgMgr,
	}
}
