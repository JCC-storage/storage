// services 包提供了服务层的封装，主要负责协调分布锁和任务管理器之间的交互。

package services

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/accessstat"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
)

// Service 结构体封装了分布锁服务和任务管理服务。
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
