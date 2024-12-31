package http

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
)

type Service struct {
	swWorker *exec.Worker
	stgAgts  *agtpool.AgentPool
}

func NewService(swWorker *exec.Worker, stgAgts *agtpool.AgentPool) *Service {
	return &Service{
		swWorker: swWorker,
		stgAgts:  stgAgts,
	}
}
