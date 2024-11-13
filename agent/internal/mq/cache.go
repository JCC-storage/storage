package mq

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) CheckCache(msg *agtmq.CheckCache) (*agtmq.CheckCacheResp, *mq.CodeMessage) {
	store, err := svc.stgMgr.GetShardStore(msg.StorageID)
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, fmt.Sprintf("get shard store of storage %v: %v", msg.StorageID, err))
	}

	infos, err := store.ListAll()
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, fmt.Sprintf("listting file in shard store: %v", err))
	}

	var fileHashes []cdssdk.FileHash
	for _, info := range infos {
		fileHashes = append(fileHashes, info.Hash)
	}

	return mq.ReplyOK(agtmq.NewCheckCacheResp(fileHashes))
}

func (svc *Service) CacheGC(msg *agtmq.CacheGC) (*agtmq.CacheGCResp, *mq.CodeMessage) {
	store, err := svc.stgMgr.GetShardStore(msg.StorageID)
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, fmt.Sprintf("get shard store of storage %v: %v", msg.StorageID, err))
	}

	err = store.GC(msg.Avaiables)
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, fmt.Sprintf("purging cache: %v", err))
	}

	return mq.ReplyOK(agtmq.RespCacheGC())
}

func (svc *Service) StartCacheMovePackage(msg *agtmq.StartCacheMovePackage) (*agtmq.StartCacheMovePackageResp, *mq.CodeMessage) {
	tsk := svc.taskManager.StartNew(mytask.NewCacheMovePackage(msg.UserID, msg.PackageID, msg.StorageID))
	return mq.ReplyOK(agtmq.NewStartCacheMovePackageResp(tsk.ID()))
}

func (svc *Service) WaitCacheMovePackage(msg *agtmq.WaitCacheMovePackage) (*agtmq.WaitCacheMovePackageResp, *mq.CodeMessage) {
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

		return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(true, errMsg))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(true, errMsg))
		}

		return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(false, ""))
	}
}
