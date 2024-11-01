package mq

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/utils"
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
		return mq.ReplyOK(agtmq.NewStorageCheckResp(
			err.Error(),
			nil,
		))
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// TODO UserID。应该设计两种接口，一种需要UserID，一种不需要。
	getStg, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{msg.StorageID}))
	if err != nil {
		return mq.ReplyOK(agtmq.NewStorageCheckResp(
			err.Error(),
			nil,
		))
	}
	if getStg.Storages[0] == nil {
		return nil, mq.Failed(errorcode.OperationFailed, "storage not found")
	}
	if getStg.Storages[0].Shared == nil {
		return nil, mq.Failed(errorcode.OperationFailed, "storage has no shared storage")
	}

	entries, err := os.ReadDir(utils.MakeStorageLoadDirectory(getStg.Storages[0].Shared.LoadBase))
	if err != nil {
		logger.Warnf("list storage directory failed, err: %s", err.Error())
		return mq.ReplyOK(agtmq.NewStorageCheckResp(
			err.Error(),
			nil,
		))
	}

	var stgPkgs []model.StoragePackage

	userDirs := lo.Filter(entries, func(info fs.DirEntry, index int) bool { return info.IsDir() })
	for _, dir := range userDirs {
		userIDInt, err := strconv.ParseInt(dir.Name(), 10, 64)
		if err != nil {
			logger.Warnf("parsing user id %s: %s", dir.Name(), err.Error())
			continue
		}

		pkgDir := filepath.Join(utils.MakeStorageLoadDirectory(getStg.Storages[0].Shared.LoadBase), dir.Name())
		pkgDirs, err := os.ReadDir(pkgDir)
		if err != nil {
			logger.Warnf("reading package dir %s: %s", pkgDir, err.Error())
			continue
		}

		for _, pkg := range pkgDirs {
			pkgIDInt, err := strconv.ParseInt(pkg.Name(), 10, 64)
			if err != nil {
				logger.Warnf("parsing package dir %s: %s", pkg.Name(), err.Error())
				continue
			}

			stgPkgs = append(stgPkgs, model.StoragePackage{
				StorageID: msg.StorageID,
				PackageID: cdssdk.PackageID(pkgIDInt),
				UserID:    cdssdk.UserID(userIDInt),
			})
		}
	}

	return mq.ReplyOK(agtmq.NewStorageCheckResp(consts.StorageDirectoryStateOK, stgPkgs))
}

func (svc *Service) StorageGC(msg *agtmq.StorageGC) (*agtmq.StorageGCResp, *mq.CodeMessage) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// TODO UserID。应该设计两种接口，一种需要UserID，一种不需要。
	getStg, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{msg.StorageID}))
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}
	if getStg.Storages[0] == nil {
		return nil, mq.Failed(errorcode.OperationFailed, "storage not found")
	}
	if getStg.Storages[0].Shared == nil {
		return nil, mq.Failed(errorcode.OperationFailed, "storage has no shared storage")
	}

	entries, err := os.ReadDir(utils.MakeStorageLoadDirectory(getStg.Storages[0].Shared.LoadBase))
	if err != nil {
		logger.Warnf("list storage directory failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "list directory files failed")
	}

	// userID->pkgID->pkg
	userPkgs := make(map[string]map[string]bool)
	for _, pkg := range msg.Packages {
		userIDStr := fmt.Sprintf("%d", pkg.UserID)

		pkgs, ok := userPkgs[userIDStr]
		if !ok {
			pkgs = make(map[string]bool)
			userPkgs[userIDStr] = pkgs
		}

		pkgIDStr := fmt.Sprintf("%d", pkg.PackageID)
		pkgs[pkgIDStr] = true
	}

	userDirs := lo.Filter(entries, func(info fs.DirEntry, index int) bool { return info.IsDir() })
	for _, dir := range userDirs {
		pkgMap, ok := userPkgs[dir.Name()]
		// 第一级目录名是UserID，先删除UserID在StoragePackage表里没出现过的文件夹
		if !ok {
			rmPath := filepath.Join(utils.MakeStorageLoadDirectory(getStg.Storages[0].Shared.LoadBase), dir.Name())
			err := os.RemoveAll(rmPath)
			if err != nil {
				logger.Warnf("removing user dir %s: %s", rmPath, err.Error())
			} else {
				logger.Debugf("user dir %s removed by gc", rmPath)
			}
			continue
		}

		pkgDir := filepath.Join(utils.MakeStorageLoadDirectory(getStg.Storages[0].Shared.LoadBase), dir.Name())
		// 遍历每个UserID目录的packages目录里的内容
		pkgs, err := os.ReadDir(pkgDir)
		if err != nil {
			logger.Warnf("reading package dir %s: %s", pkgDir, err.Error())
			continue
		}

		for _, pkg := range pkgs {
			if !pkgMap[pkg.Name()] {
				rmPath := filepath.Join(pkgDir, pkg.Name())
				err := os.RemoveAll(rmPath)
				if err != nil {
					logger.Warnf("removing package dir %s: %s", rmPath, err.Error())
				} else {
					logger.Debugf("package dir %s removed by gc", rmPath)
				}
			}
		}
	}

	return mq.ReplyOK(agtmq.RespStorageGC())
}

func (svc *Service) StartStorageCreatePackage(msg *agtmq.StartStorageCreatePackage) (*agtmq.StartStorageCreatePackageResp, *mq.CodeMessage) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		logger.Warnf("new coordinator client: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "new coordinator client failed")
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getStg, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{msg.StorageID}))
	if err != nil {
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}
	if getStg.Storages[0] == nil {
		return nil, mq.Failed(errorcode.OperationFailed, "storage not found")
	}
	if getStg.Storages[0].Shared == nil {
		return nil, mq.Failed(errorcode.OperationFailed, "storage has no shared storage")
	}

	fullPath := filepath.Clean(filepath.Join(getStg.Storages[0].Shared.LoadBase, msg.Path))

	var uploadFilePathes []string
	err = filepath.WalkDir(fullPath, func(fname string, fi os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if !fi.IsDir() {
			uploadFilePathes = append(uploadFilePathes, fname)
		}

		return nil
	})
	if err != nil {
		logger.Warnf("opening directory %s: %s", fullPath, err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "read directory failed")
	}

	objIter := iterator.NewUploadingObjectIterator(fullPath, uploadFilePathes)
	tsk := svc.taskManager.StartNew(mytask.NewCreatePackage(msg.UserID, msg.BucketID, msg.Name, objIter, msg.NodeAffinity))
	return mq.ReplyOK(agtmq.NewStartStorageCreatePackageResp(tsk.ID()))
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
