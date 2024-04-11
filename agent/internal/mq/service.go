package mq

import (
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

// Service 表示一个消息队列服务
// 它包含了任务管理和IO切换器两个核心组件
type Service struct {
	taskManager *task.Manager    // taskManager 用于管理和调度任务
	sw          *ioswitch.Switch // sw 用于控制IO切换
}

// NewService 创建一个新的消息队列服务实例
// 参数：
// - taskMgr：任务管理器，负责任务的调度和管理
// - sw：IO切换器，用于控制数据的输入输出
// 返回值：
// - *Service：指向创建的消息队列服务实例的指针
func NewService(taskMgr *task.Manager, sw *ioswitch.Switch) *Service {
	return &Service{
		taskManager: taskMgr,
		sw:          sw,
	}
}
