package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

// ExecuteIOPlan 用于执行I/O计划的任务结构体
// 临时使用Task来等待Plan执行进度
type ExecuteIOPlan struct {
	PlanID ioswitch.PlanID     // 计划ID
	Result ioswitch.PlanResult // 执行结果
}

// NewExecuteIOPlan 创建一个新的ExecuteIOPlan实例
// 参数:
//
//	planID: 要执行的I/O计划的ID
//
// 返回值:
//
//	*ExecuteIOPlan: 新创建的ExecuteIOPlan实例的指针
func NewExecuteIOPlan(planID ioswitch.PlanID) *ExecuteIOPlan {
	return &ExecuteIOPlan{
		PlanID: planID,
	}
}

// Execute 执行I/O计划
// 参数:
//
//	task: 任务实例
//	ctx: 任务执行上下文
//	complete: 完成回调函数
//
// 说明:
//
//	此函数开始执行指定的I/O计划，并通过回调函数报告完成状态
func (t *ExecuteIOPlan) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	// 记录任务日志
	log := logger.WithType[ExecuteIOPlan]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end") // 确保日志记录任务结束

	// 执行I/O计划
	ret, err := ctx.sw.ExecutePlan(t.PlanID)
	if err != nil {
		// 执行计划失败，记录警告日志并调用完成回调函数
		err := fmt.Errorf("executing io plan: %w", err)
		log.WithField("PlanID", t.PlanID).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute, // 设置延迟删除选项
		})
		return
	}

	// 计划执行成功，更新结果并调用完成回调函数
	t.Result = ret

	complete(nil, CompleteOption{
		RemovingDelay: time.Minute, // 设置延迟删除选项
	})
}
