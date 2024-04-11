package mq

import (
	"time"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

// SetupIOPlan 设置I/O计划。
// msg: 包含I/O计划信息的消息体。
// 返回值: 成功时返回响应消息和成功标志，失败时返回错误代码和消息。
func (svc *Service) SetupIOPlan(msg *agtmq.SetupIOPlan) (*agtmq.SetupIOPlanResp, *mq.CodeMessage) {
	err := svc.sw.SetupPlan(msg.Plan)
	if err != nil {
		logger.WithField("PlanID", msg.Plan.ID).Warnf("adding plan: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "adding plan failed")
	}

	return mq.ReplyOK(agtmq.NewSetupIOPlanResp())
}

// StartIOPlan 启动I/O计划。
// msg: 包含I/O计划ID的消息体。
// 返回值: 成功时返回任务ID和成功标志，失败时返回错误代码和消息。
func (svc *Service) StartIOPlan(msg *agtmq.StartIOPlan) (*agtmq.StartIOPlanResp, *mq.CodeMessage) {
	tsk := svc.taskManager.StartNew(mytask.NewExecuteIOPlan(msg.PlanID))
	return mq.ReplyOK(agtmq.NewStartIOPlanResp(tsk.ID()))
}

// WaitIOPlan 等待I/O计划完成。
// msg: 包含任务ID和等待超时时间的消息体。
// 返回值: 成功时返回任务完成状态、错误消息和结果，失败时返回错误代码和消息。
func (svc *Service) WaitIOPlan(msg *agtmq.WaitIOPlan) (*agtmq.WaitIOPlanResp, *mq.CodeMessage) {
	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error()
		}

		planTsk := tsk.Body().(*mytask.ExecuteIOPlan)
		return mq.ReplyOK(agtmq.NewWaitIOPlanResp(true, errMsg, planTsk.Result))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			planTsk := tsk.Body().(*mytask.ExecuteIOPlan)
			return mq.ReplyOK(agtmq.NewWaitIOPlanResp(true, errMsg, planTsk.Result))
		}

		return mq.ReplyOK(agtmq.NewWaitIOPlanResp(false, "", ioswitch.PlanResult{}))
	}
}

// CancelIOPlan 取消I/O计划。
// msg: 包含要取消的I/O计划ID的消息体。
// 返回值: 成功时返回响应消息和成功标志，失败时返回错误代码和消息。
func (svc *Service) CancelIOPlan(msg *agtmq.CancelIOPlan) (*agtmq.CancelIOPlanResp, *mq.CodeMessage) {
	svc.sw.CancelPlan(msg.PlanID)
	return mq.ReplyOK(agtmq.NewCancelIOPlanResp())
}
