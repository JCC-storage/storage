package http

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/mgr"
)

type Service struct {
	swWorker *exec.Worker
	stgMgr   *mgr.Manager
}

func NewService(swWorker *exec.Worker, stgMgr *mgr.Manager) *Service {
	return &Service{
		swWorker: swWorker,
		stgMgr:   stgMgr,
	}
}
