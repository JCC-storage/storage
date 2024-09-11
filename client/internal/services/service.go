package services

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/accessstat"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
)

type Service struct {
	DistLock   *distlock.Service
	TaskMgr    *task.Manager
	Downloader *downloader.Downloader
	AccessStat *accessstat.AccessStat
}

func NewService(distlock *distlock.Service, taskMgr *task.Manager, downloader *downloader.Downloader, accStat *accessstat.AccessStat) (*Service, error) {
	return &Service{
		DistLock:   distlock,
		TaskMgr:    taskMgr,
		Downloader: downloader,
		AccessStat: accStat,
	}, nil
}
