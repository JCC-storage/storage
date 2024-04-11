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
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/utils"
)

// StartStorageLoadPackage 启动存储加载包任务
// 参数:
// - msg: 包含启动存储加载包任务所需信息的消息对象，包括用户ID、包ID和存储ID
// 返回值:
// - *agtmq.StartStorageLoadPackageResp: 任务启动成功的响应对象，包含任务ID
// - *mq.CodeMessage: 任务启动失败时的错误信息对象，包含错误码和错误消息
func (svc *Service) StartStorageLoadPackage(msg *agtmq.StartStorageLoadPackage) (*agtmq.StartStorageLoadPackageResp, *mq.CodeMessage) {
	// 在任务管理器中启动一个新的存储加载包任务，并获取任务ID
	tsk := svc.taskManager.StartNew(mytask.NewStorageLoadPackage(msg.UserID, msg.PackageID, msg.StorageID))
	// 构造并返回任务启动成功的响应消息，包含任务ID
	return mq.ReplyOK(agtmq.NewStartStorageLoadPackageResp(tsk.ID()))
}

// WaitStorageLoadPackage 等待存储加载包的任务完成。
//
// 参数:
//
//	msg *agtmq.WaitStorageLoadPackage: 包含任务ID和可选的等待超时时间的消息。
//
// 返回值:
//
//	*agtmq.WaitStorageLoadPackageResp: 如果任务找到且已完成（或超时），则返回任务的响应信息，包括是否成功、错误信息和完整输出路径。
//	*mq.CodeMessage: 如果任务未找到，则返回错误代码和消息。
func (svc *Service) WaitStorageLoadPackage(msg *agtmq.WaitStorageLoadPackage) (*agtmq.WaitStorageLoadPackageResp, *mq.CodeMessage) {
	logger.WithField("TaskID", msg.TaskID).Debugf("wait loading package") // 记录等待加载包任务的debug信息

	tsk := svc.taskManager.FindByID(msg.TaskID) // 根据任务ID查找任务
	if tsk == nil {
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found") // 如果任务未找到，返回任务未找到的错误信息
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait() // 如果没有设置等待超时，那么就无限等待任务完成

		errMsg := "" // 初始化错误信息为空
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error() // 如果任务有错误，记录错误信息
		}

		loadTsk := tsk.Body().(*mytask.StorageLoadPackage) // 将任务体转换为存储加载包类型

		return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(true, errMsg, loadTsk.FullOutputPath)) // 返回任务完成的状态，错误信息和完整输出路径
	} else {
		// 如果设置了等待超时，就设置超时时间等待任务完成
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {

			errMsg := "" // 初始化错误信息为空
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error() // 如果任务有错误，记录错误信息
			}

			loadTsk := tsk.Body().(*mytask.StorageLoadPackage) // 将任务体转换为存储加载包类型

			return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(true, errMsg, loadTsk.FullOutputPath)) // 返回任务完成的状态，错误信息和完整输出路径
		}

		return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(false, "", "")) // 如果等待超时，返回任务未完成的状态
	}
}

// StorageCheck 对指定目录进行存储检查
// 参数:
// - msg: 包含需要检查的存储目录信息
// 返回值:
// - *agtmq.StorageCheckResp: 存储检查响应，包含检查结果和存储包信息
// - *mq.CodeMessage: 错误信息，如果操作成功，则为nil
func (svc *Service) StorageCheck(msg *agtmq.StorageCheck) (*agtmq.StorageCheckResp, *mq.CodeMessage) {
	// 尝试读取指定的目录
	infos, err := os.ReadDir(msg.Directory)
	if err != nil {
		// 如果读取目录失败，记录警告信息，并返回错误信息和空的存储包列表
		logger.Warnf("list storage directory failed, err: %s", err.Error())
		return mq.ReplyOK(agtmq.NewStorageCheckResp(
			err.Error(),
			nil,
		))
	}

	var stgPkgs []model.StoragePackage

	// 过滤出目录中的子目录（用户目录）
	userDirs := lo.Filter(infos, func(info fs.DirEntry, index int) bool { return info.IsDir() })
	for _, dir := range userDirs {
		// 尝试将子目录名称解析为用户ID
		userIDInt, err := strconv.ParseInt(dir.Name(), 10, 64)
		if err != nil {
			// 如果解析失败，记录警告信息，并继续处理下一个目录
			logger.Warnf("parsing user id %s: %s", dir.Name(), err.Error())
			continue
		}

		// 构造存储包目录路径，并读取该目录
		pkgDir := utils.MakeStorageLoadDirectory(msg.Directory, dir.Name())
		pkgDirs, err := os.ReadDir(pkgDir)
		if err != nil {
			// 如果读取目录失败，记录警告信息，并继续处理下一个用户目录
			logger.Warnf("reading package dir %s: %s", pkgDir, err.Error())
			continue
		}

		// 遍历存储包目录中的包，解析包ID，并添加到存储包列表中
		for _, pkg := range pkgDirs {
			pkgIDInt, err := strconv.ParseInt(pkg.Name(), 10, 64)
			if err != nil {
				// 如果解析失败，记录警告信息，并继续处理下一个包
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

	// 返回存储检查成功的响应，包含存储包列表
	return mq.ReplyOK(agtmq.NewStorageCheckResp(consts.StorageDirectoryStateOK, stgPkgs))
}

// StorageGC 执行存储垃圾回收
// 根据提供的目录和包信息，清理不再需要的文件和目录。
//
// 参数:
//
//	msg *agtmq.StorageGC: 包含需要进行垃圾回收的目录和包信息。
//
// 返回值:
//
//	*agtmq.StorageGCResp: 垃圾回收操作的响应信息。
//	*mq.CodeMessage: 如果操作失败，返回错误代码和消息。
func (svc *Service) StorageGC(msg *agtmq.StorageGC) (*agtmq.StorageGCResp, *mq.CodeMessage) {
	// 尝试列出指定目录下的所有文件和目录
	infos, err := os.ReadDir(msg.Directory)
	if err != nil {
		// 如果列出失败，记录日志并返回操作失败信息
		logger.Warnf("list storage directory failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "list directory files failed")
	}

	// 构建用户ID到包ID的映射，以便知道哪些包是需要保留的
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

	// 过滤出目录条目，并遍历这些目录
	userDirs := lo.Filter(infos, func(info fs.DirEntry, index int) bool { return info.IsDir() })
	for _, dir := range userDirs {
		pkgMap, ok := userPkgs[dir.Name()]
		// 如果当前目录在需要保留的包映射中不存在，则删除该目录
		if !ok {
			rmPath := filepath.Join(msg.Directory, dir.Name())
			err := os.RemoveAll(rmPath)
			if err != nil {
				logger.Warnf("removing user dir %s: %s", rmPath, err.Error())
			} else {
				logger.Debugf("user dir %s removed by gc", rmPath)
			}
			continue
		}

		// 遍历每个用户目录下的packages目录，删除不在保留包映射中的包
		pkgDir := utils.MakeStorageLoadDirectory(msg.Directory, dir.Name())
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

	// 垃圾回收完成，返回成功响应
	return mq.ReplyOK(agtmq.RespStorageGC())
}

// StartStorageCreatePackage 开始创建存储包的任务。
// 接收一个启动存储创建包的消息，并返回任务响应或错误消息。
//
// 参数:
//
//	msg *agtmq.StartStorageCreatePackage - 包含创建存储包所需信息的消息。
//
// 返回值:
//
//	*agtmq.StartStorageCreatePackageResp - 创建任务成功的响应，包含任务ID。
//	*mq.CodeMessage - 创建任务失败时返回的错误信息。
func (svc *Service) StartStorageCreatePackage(msg *agtmq.StartStorageCreatePackage) (*agtmq.StartStorageCreatePackageResp, *mq.CodeMessage) {
	// 从协调器MQ池获取客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		// 如果获取客户端失败，记录警告并返回错误消息
		logger.Warnf("new coordinator client: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "new coordinator client failed")
	}
	// 确保在函数结束时释放协调器MQ客户端
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 获取存储信息
	getStgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(msg.UserID, msg.StorageID))
	if err != nil {
		// 如果获取存储信息失败，记录警告并返回错误消息
		logger.WithField("StorageID", msg.StorageID).
			Warnf("getting storage info: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get storage info failed")
	}

	// 计算打包文件的完整路径
	fullPath := filepath.Clean(filepath.Join(getStgResp.Directory, msg.Path))

	// 遍历目录，收集所有需要上传的文件路径
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
		// 如果目录读取失败，记录警告并返回错误消息
		logger.Warnf("opening directory %s: %s", fullPath, err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "read directory failed")
	}

	// 创建上传对象的迭代器
	objIter := iterator.NewUploadingObjectIterator(fullPath, uploadFilePathes)
	// 启动新任务来创建存储包
	tsk := svc.taskManager.StartNew(mytask.NewCreatePackage(msg.UserID, msg.BucketID, msg.Name, objIter, msg.NodeAffinity))
	// 返回任务成功的响应
	return mq.ReplyOK(agtmq.NewStartStorageCreatePackageResp(tsk.ID()))
}

// WaitStorageCreatePackage 等待存储创建包的处理函数。
//
// 参数:
// msg: 包含任务ID和等待超时时间的消息对象。
//
// 返回值:
// 返回一个任务响应对象和一个错误消息对象。如果任务找到且未超时，将返回任务的结果；如果任务未找到或超时，将返回相应的错误信息。
func (svc *Service) WaitStorageCreatePackage(msg *agtmq.WaitStorageCreatePackage) (*agtmq.WaitStorageCreatePackageResp, *mq.CodeMessage) {
	// 根据任务ID查找任务
	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		// 如果任务未找到，返回任务未找到错误
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
	}

	// 根据等待超时时间进行等待处理
	if msg.WaitTimeoutMs == 0 {
		// 如果没有设置超时时间，无限等待
		tsk.Wait()
	} else if !tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {
		// 如果设置了超时时间，且超时未完成，返回超时处理结果
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(false, "", 0))
	}

	// 检查任务是否有错误
	if tsk.Error() != nil {
		// 如果任务有错误，返回错误信息
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, tsk.Error().Error(), 0))
	}

	// 获取任务结果
	taskBody := tsk.Body().(*mytask.CreatePackage)
	// 返回任务成功处理结果
	return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, "", taskBody.Result.PackageID))
}
