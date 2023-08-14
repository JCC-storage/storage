package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage-client/internal/config"
	agtcli "gitlink.org.cn/cloudream/storage-common/pkgs/mq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

type MoveObjectToStorage struct {
	userID    int64
	objectID  int64
	storageID int64
}

func NewMoveObjectToStorage(userID int64, objectID int64, storageID int64) *MoveObjectToStorage {
	return &MoveObjectToStorage{
		userID:    userID,
		objectID:  objectID,
		storageID: storageID,
	}
}

func (t *MoveObjectToStorage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *MoveObjectToStorage) do(ctx TaskContext) error {
	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有Storage权限
		UserStorage().ReadOne(t.objectID, t.storageID).
		// 用于判断用户是否有对象权限
		UserBucket().ReadAny().
		// 用于读取对象信息
		Object().ReadOne(t.objectID).
		// 用于查询Rep配置
		ObjectRep().ReadOne(t.objectID).
		// 用于查询Block配置
		ObjectBlock().ReadAny().
		// 用于创建Move记录
		StorageObject().CreateOne(t.storageID, t.userID, t.objectID).
		Storage().
		// 用于创建对象文件
		CreateOneObject(t.storageID, t.userID, t.objectID).
		MutexLock(ctx.DistLock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()
	err = moveSingleObjectToStorage(ctx, t.userID, t.objectID, t.storageID)
	return err
}

func moveSingleObjectToStorage(ctx TaskContext, userID int64, objectID int64, storageID int64) error {
	// 先向协调端请求文件相关的元数据
	preMoveResp, err := ctx.Coordinator.PreMoveObjectToStorage(coormsg.NewPreMoveObjectToStorage(objectID, storageID, userID))
	if err != nil {
		return fmt.Errorf("pre move object to storage: %w", err)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := agtcli.NewClient(preMoveResp.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", preMoveResp.NodeID, err)
	}
	defer agentClient.Close()

	agentMoveResp, err := agentClient.StartStorageMoveObject(
		agtmsg.NewStartStorageMoveObject(preMoveResp.Directory,
			objectID,
			preMoveResp.Object.Name,
			userID,
			preMoveResp.Object.FileSize,
			preMoveResp.Redundancy,
		))
	if err != nil {
		return fmt.Errorf("start moving object to storage: %w", err)
	}

	for {
		waitResp, err := agentClient.WaitStorageMoveObject(agtmsg.NewWaitStorageMoveObject(agentMoveResp.TaskID, int64(time.Second)*5))
		if err != nil {
			return fmt.Errorf("wait moving object: %w", err)
		}

		if waitResp.IsComplete {
			if waitResp.Error != "" {
				return fmt.Errorf("agent moving object: %s", waitResp.Error)
			}

			break
		}
	}

	_, err = ctx.Coordinator.MoveObjectToStorage(coormsg.NewMoveObjectToStorage(objectID, storageID, userID))
	if err != nil {
		return fmt.Errorf("moving object to storage: %w", err)
	}
	return nil
}
