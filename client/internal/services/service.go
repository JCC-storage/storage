package services

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	packagestat "gitlink.org.cn/cloudream/storage/common/pkgs/package_stat"
)

type Service struct {
	DistLock    *distlock.Service
	TaskMgr     *task.Manager
	Downloader  *downloader.Downloader
	PackageStat *packagestat.PackageStat
}

func NewService(distlock *distlock.Service, taskMgr *task.Manager, downloader *downloader.Downloader, pkgStat *packagestat.PackageStat) (*Service, error) {
	return &Service{
		DistLock:    distlock,
		TaskMgr:     taskMgr,
		Downloader:  downloader,
		PackageStat: pkgStat,
	}, nil
}
