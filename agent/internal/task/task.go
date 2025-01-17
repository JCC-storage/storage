package task

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/accessstat"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
	"gitlink.org.cn/cloudream/storage/common/pkgs/uploader"
)

// TaskContext 定义了任务执行的上下文环境，包含分布式锁服务、IO开关和网络连接状态收集器
type TaskContext struct {
	distlock     *distlock.Service
	connectivity *connectivity.Collector
	downloader   *downloader.Downloader
	accessStat   *accessstat.AccessStat
	stgAgts      *agtpool.AgentPool
	uploader     *uploader.Uploader
}

// CompleteFn 类型定义了任务完成时需要执行的函数，用于设置任务的执行结果
type CompleteFn = task.CompleteFn

// Manager 类型代表任务管理器，用于创建、管理和调度任务
type Manager = task.Manager[TaskContext]

// TaskBody 类型定义了任务体，包含了任务的具体执行逻辑
type TaskBody = task.TaskBody[TaskContext]

// Task 类型代表一个具体的任务，包含了任务的上下文、执行体和其它相关信息
type Task = task.Task[TaskContext]

// CompleteOption 类型定义了任务完成时的选项，可用于定制化任务完成的处理方式
type CompleteOption = task.CompleteOption

func NewManager(distlock *distlock.Service, connectivity *connectivity.Collector, downloader *downloader.Downloader, accessStat *accessstat.AccessStat, stgAgts *agtpool.AgentPool, uploader *uploader.Uploader) Manager {
	return task.NewManager(TaskContext{
		distlock:     distlock,
		connectivity: connectivity,
		downloader:   downloader,
		accessStat:   accessStat,
		stgAgts:      stgAgts,
		uploader:     uploader,
	})
}
