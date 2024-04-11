package mq

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

// PinObject 用于处理对象固定（pin）的请求。
// msg: 包含要固定的对象的文件哈希和是否为后台任务的标志。
// 返回值1: 成功时返回固定操作的响应信息。
// 返回值2: 操作失败时返回错误码和错误信息。
func (svc *Service) PinObject(msg *agtmq.PinObject) (*agtmq.PinObjectResp, *mq.CodeMessage) {
	// 开始记录固定对象操作的日志
	logger.WithField("FileHash", msg.FileHashes).Debugf("pin object")

	// 启动一个新的任务来处理IPFS固定操作
	tsk := svc.taskManager.StartNew(task.NewIPFSPin(msg.FileHashes))

	// 检查任务是否出错，若有错误则记录日志并返回操作失败的信息
	if tsk.Error() != nil {
		logger.WithField("FileHash", msg.FileHashes).
			Warnf("pin object failed, err: %s", tsk.Error().Error())
		return nil, mq.Failed(errorcode.OperationFailed, "pin object failed")
	}

	// 如果是后台任务，则直接返回成功响应，不等待任务完成
	if msg.IsBackground {
		return mq.ReplyOK(agtmq.RespPinObject())
	}

	// 等待任务完成
	tsk.Wait()
	return mq.ReplyOK(agtmq.RespPinObject())
}
