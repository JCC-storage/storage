// services 包提供了服务层的封装，主要负责协调分布锁和任务管理器之间的交互。

package services

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"         // 导入分布锁服务包
	"gitlink.org.cn/cloudream/storage/client/internal/task" // 导入任务管理服务包
)

// Service 结构体封装了分布锁服务和任务管理服务。
type Service struct {
	DistLock *distlock.Service // DistLock 用于分布式环境下的锁服务
	TaskMgr  *task.Manager     // TaskMgr 用于任务的创建、管理和执行
}

// NewService 创建一个新的Service实例。
//
// 参数:
//
//	distlock *distlock.Service: 分布式锁服务的实例。
//	taskMgr *task.Manager: 任务管理器的实例。
//
// 返回值:
//
//	*Service: 初始化后的Service实例。
//	error: 如果创建过程中遇到错误，则返回错误信息，否则为nil。
func NewService(distlock *distlock.Service, taskMgr *task.Manager) (*Service, error) {
	return &Service{
		DistLock: distlock,
		TaskMgr:  taskMgr,
	}, nil
}
