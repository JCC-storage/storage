package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// CacheMovePackage 代表缓存移动包的任务实体。
type CacheMovePackage struct {
	userID    cdssdk.UserID    // 用户ID
	packageID cdssdk.PackageID // 包ID
}

// NewCacheMovePackage 创建一个新的缓存移动包任务实例。
func NewCacheMovePackage(userID cdssdk.UserID, packageID cdssdk.PackageID) *CacheMovePackage {
	return &CacheMovePackage{
		userID:    userID,
		packageID: packageID,
	}
}

// Execute 执行缓存移动包的任务。
// task: 任务实例。
// ctx: 任务上下文。
// complete: 任务完成的回调函数。
func (t *CacheMovePackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

// do 实际执行缓存移动的逻辑。
func (t *CacheMovePackage) do(ctx TaskContext) error {
	log := logger.WithType[CacheMovePackage]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	// 获取分布式锁以保护操作
	mutex, err := reqbuilder.NewBuilder().
		IPFS().Buzy(*stgglb.Local.NodeID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquiring distlock: %w", err)
	}
	defer mutex.Unlock()

	// 获取协调器MQ客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 获取包内对象详情
	getResp, err := coorCli.GetPackageObjectDetails(coormq.NewGetPackageObjectDetails(t.packageID))
	if err != nil {
		return fmt.Errorf("getting package object details: %w", err)
	}

	// 获取IPFS客户端
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close()

	// 遍历并下载对象
	objIter := iterator.NewDownloadObjectIterator(getResp.Objects, &iterator.DownloadContext{
		Distlock: ctx.distlock,
	})
	defer objIter.Close()

	for {
		obj, err := objIter.MoveNext()
		if err != nil {
			if err == iterator.ErrNoMoreItem {
				break
			}
			return err
		}
		defer obj.File.Close()

		// 将对象文件添加到IPFS
		_, err = ipfsCli.CreateFile(obj.File)
		if err != nil {
			return fmt.Errorf("creating ipfs file: %w", err)
		}
	}

	// 通知协调器缓存已移动
	_, err = coorCli.CachePackageMoved(coormq.NewCachePackageMoved(t.packageID, *stgglb.Local.NodeID))
	if err != nil {
		return fmt.Errorf("request to coordinator: %w", err)
	}

	return nil
}
