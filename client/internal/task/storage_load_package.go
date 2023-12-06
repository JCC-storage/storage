package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// TODO 可以考虑不用Task来实现这些逻辑
type StorageLoadPackage struct {
	userID    cdssdk.UserID
	packageID cdssdk.PackageID
	storageID cdssdk.StorageID

	ResultFullPath string
}

func NewStorageLoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID) *StorageLoadPackage {
	return &StorageLoadPackage{
		userID:    userID,
		packageID: packageID,
		storageID: storageID,
	}
}

func (t *StorageLoadPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *StorageLoadPackage) do(ctx TaskContext) error {
	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有Storage权限
		UserStorage().ReadOne(t.userID, t.storageID).
		// 用于判断用户是否有对象权限
		UserBucket().ReadAny().
		// 用于读取包信息
		Package().ReadOne(t.packageID).
		// 用于读取对象信息
		Object().ReadAny().
		// 用于查询Rep配置
		ObjectRep().ReadAny().
		// 用于查询Block配置
		ObjectBlock().ReadAny().
		// 用于创建Move记录
		StoragePackage().CreateOne(t.storageID, t.userID, t.packageID).
		Storage().
		// 用于创建对象文件
		CreateOnePackage(t.storageID, t.userID, t.packageID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getStgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(t.userID, t.storageID))
	if err != nil {
		return fmt.Errorf("getting storage info: %w", err)
	}

	// 然后向代理端发送移动文件的请求
	agentCli, err := stgglb.AgentMQPool.Acquire(getStgResp.NodeID)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", getStgResp.NodeID, err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	agentMoveResp, err := agentCli.StartStorageLoadPackage(
		agtmq.NewStartStorageLoadPackage(
			t.userID,
			t.packageID,
			t.storageID,
		))
	if err != nil {
		return fmt.Errorf("start loading package to storage: %w", err)
	}

	for {
		waitResp, err := agentCli.WaitStorageLoadPackage(agtmq.NewWaitStorageLoadPackage(agentMoveResp.TaskID, int64(time.Second)*5))
		if err != nil {
			return fmt.Errorf("wait loading package: %w", err)
		}

		if waitResp.IsComplete {
			if waitResp.Error != "" {
				return fmt.Errorf("agent loading package: %s", waitResp.Error)
			}

			t.ResultFullPath = waitResp.FullPath
			break
		}
	}

	_, err = coorCli.StoragePackageLoaded(coormq.NewStoragePackageLoaded(t.userID, t.storageID, t.packageID))
	if err != nil {
		return fmt.Errorf("loading package to storage: %w", err)
	}
	return nil
}
