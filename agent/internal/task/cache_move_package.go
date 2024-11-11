package task

import (
	"fmt"
	"io"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type CacheMovePackage struct {
	userID    cdssdk.UserID
	packageID cdssdk.PackageID
	storageID cdssdk.StorageID
}

func NewCacheMovePackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID) *CacheMovePackage {
	return &CacheMovePackage{
		userID:    userID,
		packageID: packageID,
		storageID: storageID,
	}
}

func (t *CacheMovePackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *CacheMovePackage) do(ctx TaskContext) error {
	log := logger.WithType[CacheMovePackage]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	store, err := ctx.stgMgr.GetShardStore(t.storageID)
	if err != nil {
		return fmt.Errorf("get shard store of storage %v: %w", t.storageID, err)
	}

	mutex, err := reqbuilder.NewBuilder().
		// 保护解码出来的Object数据
		Shard().Buzy(t.storageID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquiring distlock: %w", err)
	}
	defer mutex.Unlock()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// TODO 可以考虑优化，比如rep类型的直接pin就可以
	objIter := ctx.downloader.DownloadPackage(t.packageID)
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

		writer := store.New()
		_, err = io.Copy(writer, obj.File)
		if err != nil {
			writer.Abort()
			return fmt.Errorf("writing to store: %w", err)
		}
		_, err = writer.Finish()
		if err != nil {
			return fmt.Errorf("finishing store: %w", err)
		}

		ctx.accessStat.AddAccessCounter(obj.Object.ObjectID, t.packageID, t.storageID, 1)
	}

	_, err = coorCli.CachePackageMoved(coormq.NewCachePackageMoved(t.packageID, t.storageID))
	if err != nil {
		return fmt.Errorf("request to coordinator: %w", err)
	}

	return nil
}
