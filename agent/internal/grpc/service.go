package grpc

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	agentserver "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/svcmgr"
)

type Service struct {
	agentserver.AgentServer
	swWorker *exec.Worker
	stgMgr   *svcmgr.Manager
}

func NewService(swWorker *exec.Worker, stgMgr *svcmgr.Manager) *Service {
	return &Service{
		swWorker: swWorker,
		stgMgr:   stgMgr,
	}
}
