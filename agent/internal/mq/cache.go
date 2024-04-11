package mq

import (
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

// CheckCache 检查IPFS缓存
// 参数 msg: 包含检查缓存请求信息的结构体
// 返回值: 检查缓存响应结构体和错误信息
func (svc *Service) CheckCache(msg *agtmq.CheckCache) (*agtmq.CheckCacheResp, *mq.CodeMessage) {
	ipfsCli, err := stgglb.IPFSPool.Acquire() // 尝试从IPFS池获取客户端
	if err != nil {
		logger.Warnf("new ipfs client: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "new ipfs client failed")
	}
	defer ipfsCli.Close() // 确保IPFS客户端被正确关闭

	files, err := ipfsCli.GetPinnedFiles() // 获取IPFS上被固定的文件列表
	if err != nil {
		logger.Warnf("get pinned files from ipfs failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get pinned files from ipfs failed")
	}

	return mq.ReplyOK(agtmq.NewCheckCacheResp(lo.Keys(files))) // 返回文件列表的键
}

// CacheGC 执行缓存垃圾回收
// 参数 msg: 包含垃圾回收请求信息的结构体
// 返回值: 垃圾回收响应结构体和错误信息
func (svc *Service) CacheGC(msg *agtmq.CacheGC) (*agtmq.CacheGCResp, *mq.CodeMessage) {
	ipfsCli, err := stgglb.IPFSPool.Acquire() // 尝试从IPFS池获取客户端
	if err != nil {
		logger.Warnf("new ipfs client: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "new ipfs client failed")
	}
	defer ipfsCli.Close() // 确保IPFS客户端被正确关闭

	files, err := ipfsCli.GetPinnedFiles() // 获取IPFS上被固定的文件列表
	if err != nil {
		logger.Warnf("get pinned files from ipfs failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get pinned files from ipfs failed")
	}

	// 根据请求对比当前被固定的文件，将未记录到元数据的文件取消固定
	shouldPinnedFiles := lo.SliceToMap(msg.PinnedFileHashes, func(hash string) (string, bool) { return hash, true })
	for hash := range files {
		if !shouldPinnedFiles[hash] {
			ipfsCli.Unpin(hash) // 取消固定文件
			logger.WithField("FileHash", hash).Debugf("unpinned by gc")
		}
	}

	return mq.ReplyOK(agtmq.RespCacheGC()) // 返回垃圾回收完成的响应
}

// StartCacheMovePackage 开始缓存移动包
// 参数 msg: 包含启动缓存移动请求信息的结构体
// 返回值: 启动缓存移动响应结构体和错误信息
func (svc *Service) StartCacheMovePackage(msg *agtmq.StartCacheMovePackage) (*agtmq.StartCacheMovePackageResp, *mq.CodeMessage) {
	tsk := svc.taskManager.StartNew(mytask.NewCacheMovePackage(msg.UserID, msg.PackageID)) // 启动新的缓存移动任务
	return mq.ReplyOK(agtmq.NewStartCacheMovePackageResp(tsk.ID()))                        // 返回任务ID
}

// WaitCacheMovePackage 等待缓存移动包完成
// 参数 msg: 包含等待缓存移动请求信息的结构体
// 返回值: 等待缓存移动响应结构体和错误信息
func (svc *Service) WaitCacheMovePackage(msg *agtmq.WaitCacheMovePackage) (*agtmq.WaitCacheMovePackageResp, *mq.CodeMessage) {
	tsk := svc.taskManager.FindByID(msg.TaskID) // 根据任务ID查找任务
	if tsk == nil {
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found") // 如果任务不存在，返回错误
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait() // 等待任务完成

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error() // 获取任务错误信息
		}

		return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(true, errMsg)) // 返回任务完成状态和错误信息

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) { // 设置等待超时

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error() // 获取任务错误信息
			}

			return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(true, errMsg)) // 返回任务完成状态和错误信息
		}

		return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(false, "")) // 返回等待超时状态
	}
}
