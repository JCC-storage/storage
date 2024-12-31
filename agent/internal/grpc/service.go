package grpc

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	agentserver "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
)

type Service struct {
	agentserver.AgentServer
	swWorker *exec.Worker
	stgAgts  *agtpool.AgentPool
}

func NewService(swWorker *exec.Worker, stgAgts *agtpool.AgentPool) *Service {
	return &Service{
		swWorker: swWorker,
		stgAgts:  stgAgts,
	}
}
