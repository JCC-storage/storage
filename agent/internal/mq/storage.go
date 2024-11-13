package mq

import (
	"time"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) StartStorageLoadPackage(msg *agtmq.StartStorageLoadPackage) (*agtmq.StartStorageLoadPackageResp, *mq.CodeMessage) {
	tsk := svc.taskManager.StartNew(mytask.NewStorageLoadPackage(msg.UserID, msg.PackageID, msg.StorageID))
	return mq.ReplyOK(agtmq.NewStartStorageLoadPackageResp(tsk.ID()))
}

func (svc *Service) WaitStorageLoadPackage(msg *agtmq.WaitStorageLoadPackage) (*agtmq.WaitStorageLoadPackageResp, *mq.CodeMessage) {
	logger.WithField("TaskID", msg.TaskID).Debugf("wait loading package")

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

		loadTsk := tsk.Body().(*mytask.StorageLoadPackage)

		return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(true, errMsg, loadTsk.PackagePath, loadTsk.LocalBase, loadTsk.RemoteBase))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			loadTsk := tsk.Body().(*mytask.StorageLoadPackage)

			return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(true, errMsg, loadTsk.PackagePath, loadTsk.LocalBase, loadTsk.RemoteBase))
		}

		return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(false, "", "", "", ""))
	}
}

func (svc *Service) StorageCheck(msg *agtmq.StorageCheck) (*agtmq.StorageCheckResp, *mq.CodeMessage) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	shared, err := svc.stgMgr.GetSharedStore(msg.StorageID)
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	loaded, err := shared.ListLoadedPackages()
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(agtmq.NewStorageCheckResp(loaded))
}

func (svc *Service) StorageGC(msg *agtmq.StorageGC) (*agtmq.StorageGCResp, *mq.CodeMessage) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	shared, err := svc.stgMgr.GetSharedStore(msg.StorageID)
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	var loadeds []stgmod.LoadedPackageID
	for _, pkg := range msg.Packages {
		loadeds = append(loadeds, stgmod.LoadedPackageID{
			UserID:    pkg.UserID,
			PackageID: pkg.PackageID,
		})
	}

	err = shared.PackageGC(loadeds)
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(agtmq.RespStorageGC())
}

func (svc *Service) StartStorageCreatePackage(msg *agtmq.StartStorageCreatePackage) (*agtmq.StartStorageCreatePackageResp, *mq.CodeMessage) {
	return nil, mq.Failed(errorcode.OperationFailed, "not implemented")
	// coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	// if err != nil {
	// 	logger.Warnf("new coordinator client: %s", err.Error())

	// 	return nil, mq.Failed(errorcode.OperationFailed, "new coordinator client failed")
	// }
	// defer stgglb.CoordinatorMQPool.Release(coorCli)

	// getStg, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{msg.StorageID}))
	// if err != nil {
	// 	return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	// }
	// if getStg.Storages[0] == nil {
	// 	return nil, mq.Failed(errorcode.OperationFailed, "storage not found")
	// }
	// if getStg.Storages[0].Shared == nil {
	// 	return nil, mq.Failed(errorcode.OperationFailed, "storage has no shared storage")
	// }

	// fullPath := filepath.Clean(filepath.Join(getStg.Storages[0].Shared.LoadBase, msg.Path))

	// var uploadFilePathes []string
	// err = filepath.WalkDir(fullPath, func(fname string, fi os.DirEntry, err error) error {
	// 	if err != nil {
	// 		return nil
	// 	}

	// 	if !fi.IsDir() {
	// 		uploadFilePathes = append(uploadFilePathes, fname)
	// 	}

	// 	return nil
	// })
	// if err != nil {
	// 	logger.Warnf("opening directory %s: %s", fullPath, err.Error())

	// 	return nil, mq.Failed(errorcode.OperationFailed, "read directory failed")
	// }

	// objIter := iterator.NewUploadingObjectIterator(fullPath, uploadFilePathes)
	// tsk := svc.taskManager.StartNew(mytask.NewCreatePackage(msg.UserID, msg.BucketID, msg.Name, objIter, msg.StorageAffinity))
	// return mq.ReplyOK(agtmq.NewStartStorageCreatePackageResp(tsk.ID()))
}

func (svc *Service) WaitStorageCreatePackage(msg *agtmq.WaitStorageCreatePackage) (*agtmq.WaitStorageCreatePackageResp, *mq.CodeMessage) {
	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()
	} else if !tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(false, "", 0))
	}

	if tsk.Error() != nil {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, tsk.Error().Error(), 0))
	}

	taskBody := tsk.Body().(*mytask.CreatePackage)
	return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, "", taskBody.Result.PackageID))
}
