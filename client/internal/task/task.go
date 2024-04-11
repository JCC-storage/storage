package task

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"             // 引入分布式锁服务
	"gitlink.org.cn/cloudream/common/pkgs/task"                 // 引入任务处理相关的包
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity" // 引入网络连接状态收集器
)

// TaskContext 定义了任务执行的上下文环境，包含分布式锁服务和网络连接状态收集器
type TaskContext struct {
	distlock     *distlock.Service
	connectivity *connectivity.Collector
}

// CompleteFn 类型定义了任务完成时的回调函数，用于设置任务的执行结果
type CompleteFn = task.CompleteFn

// Manager 类型定义了任务管理器，用于创建、管理和调度任务
type Manager = task.Manager[TaskContext]

// TaskBody 类型定义了任务的主体部分，包含了任务实际执行的逻辑
type TaskBody = task.TaskBody[TaskContext]

// Task 类型定义了具体的任务，包括任务的上下文、主体和完成选项
type Task = task.Task[TaskContext]

// CompleteOption 类型定义了任务完成时的选项，可用于定制任务完成的处理方式
type CompleteOption = task.CompleteOption

// NewManager 创建一个新的任务管理器实例，接受一个分布式锁服务和一个网络连接状态收集器作为参数
// 返回一个初始化好的任务管理器实例
func NewManager(distlock *distlock.Service, connectivity *connectivity.Collector) Manager {
	return task.NewManager(TaskContext{
		distlock:     distlock,
		connectivity: connectivity,
	})
}
