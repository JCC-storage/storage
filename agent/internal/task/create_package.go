package task

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// CreatePackageResult 定义创建包的结果结构
// 包含包的ID和上传的对象列表
type CreatePackageResult struct {
	PackageID cdssdk.PackageID
	Objects   []cdssdk.Object
}

// CreatePackage 定义创建包的任务结构
// 包含用户ID、存储桶ID、包名称、上传对象的迭代器、节点亲和性以及任务结果
type CreatePackage struct {
	userID      cdssdk.UserID
	bucketID    cdssdk.BucketID
	name        string
	objIter     iterator.UploadingObjectIterator
	stgAffinity cdssdk.StorageID
	Result      CreatePackageResult
}

// NewCreatePackage 创建一个新的CreatePackage实例
// userID: 用户ID
// bucketID: 存储桶ID
// name: 包名称
// objIter: 上传对象的迭代器
// stgAffinity: 节点亲和性，指定包应该创建在哪个节点上（可选）
// 返回CreatePackage实例的指针
func NewCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, objIter iterator.UploadingObjectIterator, stgAffinity cdssdk.StorageID) *CreatePackage {
	return &CreatePackage{
		userID:      userID,
		bucketID:    bucketID,
		name:        name,
		objIter:     objIter,
		stgAffinity: stgAffinity,
	}
}

// Execute 执行创建包的任务
// task: 任务实例，携带任务上下文
// ctx: 任务上下文，包含分布式锁和网络连接性等信息
// complete: 任务完成的回调函数
func (t *CreatePackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	// 获取任务日志记录器
	log := logger.WithType[CreatePackage]("Task")

	log.Debugf("begin")
	defer log.Debugf("end")

	// 从MQ池中获取协调器客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		err = fmt.Errorf("new coordinator client: %w", err)
		log.Warn(err.Error())
		// 完成任务并设置移除延迟
		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器创建包
	createResp, err := coorCli.CreatePackage(coordinator.NewCreatePackage(t.userID, t.bucketID, t.name))
	if err != nil {
		err = fmt.Errorf("creating package: %w", err)
		log.Error(err.Error())
		// 完成任务并设置移除延迟
		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	up, err := ctx.uploader.BeginUpdate(t.userID, createResp.Package.PackageID, t.stgAffinity, nil, nil)
	if err != nil {
		err = fmt.Errorf("begin update: %w", err)
		log.Error(err.Error())
		// 完成任务并设置移除延迟
		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer up.Abort()

	for {
		obj, err := t.objIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			log.Error(err.Error())
			// 完成任务并设置移除延迟
			complete(err, CompleteOption{
				RemovingDelay: time.Minute,
			})
			return
		}
		path := filepath.ToSlash(obj.Path)

		// 上传对象
		err = up.Upload(path, obj.File)
		if err != nil {
			err = fmt.Errorf("uploading object: %w", err)
			log.Error(err.Error())
			// 完成任务并设置移除延迟
			complete(err, CompleteOption{
				RemovingDelay: time.Minute,
			})
			return
		}

	}

	// 结束上传
	uploadRet, err := up.Commit()
	if err != nil {
		err = fmt.Errorf("uploading objects: %w", err)
		log.Error(err.Error())
		// 完成任务并设置移除延迟
		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	t.Result.PackageID = createResp.Package.PackageID
	t.Result.Objects = lo.Values(uploadRet.Objects)

	// 完成任务并设置移除延迟
	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
