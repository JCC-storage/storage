// package task 定义了与任务处理相关的结构体和函数。
package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/task"             // 引入task包，提供任务处理的通用功能。
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"   // 引入cdssdk包，提供云存储相关的SDK接口。
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"      // 引入cmd包，提供命令执行相关的功能。
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator" // 引入iterator包，提供迭代器相关的功能。
)

// UploadObjectsResult 定义了上传对象结果的类型，继承自cmd包的UploadObjectsResult类型。
type UploadObjectsResult = cmd.UploadObjectsResult

// UploadObjects 定义了上传对象的任务结构体，包含上传命令和执行结果。
type UploadObjects struct {
	cmd cmd.UploadObjects // cmd字段定义了上传对象的具体操作。

	Result *UploadObjectsResult // Result字段存储上传对象操作的结果。
}

// NewUploadObjects 创建并返回一个新的UploadObjects实例。
// userID: 用户ID，标识发起上传请求的用户。
// packageID: 包ID，标识被上传的对象所属的包。
// objectIter: 上传对象迭代器，用于遍历和上传多个对象。
// nodeAffinity: 节点亲和性，指定上传任务首选的执行节点。
// 返回值为初始化后的UploadObjects指针。
func NewUploadObjects(userID cdssdk.UserID, packageID cdssdk.PackageID, objectIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) *UploadObjects {
	return &UploadObjects{
		cmd: *cmd.NewUploadObjects(userID, packageID, objectIter, nodeAffinity),
	}
}

// Execute 执行上传对象的任务。
// task: 任务实例，包含任务的上下文信息。
// ctx: 任务执行的上下文，包括分布式锁和网络连接性等信息。
// complete: 任务完成时的回调函数。
// 该函数负责调用上传命令的Execute方法，处理上传结果，并通过回调函数报告任务完成情况。
func (t *UploadObjects) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	ret, err := t.cmd.Execute(&cmd.UploadObjectsContext{
		Distlock:     ctx.distlock,     // 使用任务上下文中的分布式锁。
		Connectivity: ctx.connectivity, // 使用任务上下文中的网络连接性信息。
	})

	t.Result = ret // 存储上传结果。

	complete(err, CompleteOption{
		RemovingDelay: time.Minute, // 设置任务完成后的清理延迟为1分钟。
	})
}
