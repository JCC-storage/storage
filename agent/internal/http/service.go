package http

import "gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"

type Service struct {
	swWorker *exec.Worker
}

func NewService(swWorker *exec.Worker) *Service {
	return &Service{
		swWorker: swWorker,
	}
}
