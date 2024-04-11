package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
)

// IPFSPin 定义了一个结构体，用于IPFS的pin操作任务。
type IPFSPin struct {
	FileHashes []string // FileHashes 存储需要pin的文件的hash列表。
}

// NewIPFSPin 创建一个新的IPFSPin实例。
// fileHashes 是一个包含需要pin的文件hash的字符串切片。
// 返回一个指向IPFSPin实例的指针。
func NewIPFSPin(fileHashes []string) *IPFSPin {
	return &IPFSPin{
		FileHashes: fileHashes,
	}
}

// Execute 执行IPFSPin任务。
// 该函数负责获取IPFS客户端，然后对FileHashes中的每个文件hash执行pin操作。
// task 是一个指向task.Task[TaskContext]的指针，代表当前的任务实例。
// ctx 是当前任务的上下文信息。
// complete 是一个完成回调函数，用于在任务结束时（成功或失败）进行一些清理工作。
func (t *IPFSPin) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	// 使用logger记录任务开始的信息。
	log := logger.WithType[IPFSPin]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end") // 确保记录任务结束的信息。

	// 尝试从IPFS池中获取一个客户端实例。
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		// 如果获取客户端失败，则使用complete函数通知任务失败，并设置移除延迟。
		err := fmt.Errorf("new ipfs client: %w", err)
		log.Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer ipfsCli.Close() // 确保在函数返回前释放IPFS客户端实例。

	// 遍历文件hash列表，并尝试对每个hash执行pin操作。
	for _, fileHash := range t.FileHashes {
		err = ipfsCli.Pin(fileHash)
		if err != nil {
			// 如果pin操作失败，则使用complete函数通知任务失败，并设置移除延迟。
			err := fmt.Errorf("pin file failed, err: %w", err)
			log.WithField("FileHash", fileHash).Warn(err.Error())

			complete(err, CompleteOption{
				RemovingDelay: time.Minute,
			})
			return
		}
	}

	// 所有文件的pin操作成功，使用complete函数通知任务成功完成，并设置移除延迟。
	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
