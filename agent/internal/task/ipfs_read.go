package task

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
)

// IPFSRead 代表从IPFS读取文件的任务
type IPFSRead struct {
	FileHash  string // 文件的IPFS哈希值
	LocalPath string // 本地存储路径
}

// NewIPFSRead 创建一个新的IPFS读取任务实例
func NewIPFSRead(fileHash string, localPath string) *IPFSRead {
	return &IPFSRead{
		FileHash:  fileHash,
		LocalPath: localPath,
	}
}

// Compare 比较当前任务与另一个任务是否相同
// other: 要比较的另一个任务
// 返回值: 如果两个任务相同返回true，否则返回false
func (t *IPFSRead) Compare(other *Task) bool {
	tsk, ok := other.Body().(*IPFSRead)
	if !ok {
		return false
	}

	return t.FileHash == tsk.FileHash && t.LocalPath == tsk.LocalPath
}

// Execute 执行从IPFS读取文件并存储到本地的任务
// task: 任务实例
// ctx: 任务上下文
// complete: 任务完成的回调函数
func (t *IPFSRead) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	// 初始化日志
	log := logger.WithType[IPFSRead]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	// 获取输出文件的目录并创建该目录
	outputFileDir := filepath.Dir(t.LocalPath)

	// 创建输出文件的目录
	err := os.MkdirAll(outputFileDir, os.ModePerm)
	if err != nil {
		// 目录创建失败的处理
		err := fmt.Errorf("create output file directory %s failed, err: %w", outputFileDir, err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	// 创建输出文件
	outputFile, err := os.Create(t.LocalPath)
	if err != nil {
		// 输出文件创建失败的处理
		err := fmt.Errorf("create output file %s failed, err: %w", t.LocalPath, err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer outputFile.Close()

	// 获取IPFS客户端
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		// 获取IPFS客户端失败的处理
		err := fmt.Errorf("new ipfs client: %w", err)
		log.Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer ipfsCli.Close()

	// 打开IPFS中的文件进行读取
	rd, err := ipfsCli.OpenRead(t.FileHash)
	if err != nil {
		// 打开IPFS文件失败的处理
		err := fmt.Errorf("read ipfs file failed, err: %w", err)
		log.WithField("FileHash", t.FileHash).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	// 将IPFS文件内容复制到本地文件
	_, err = io.Copy(outputFile, rd)
	if err != nil {
		// 文件复制失败的处理
		err := fmt.Errorf("copy ipfs file to local file failed, err: %w", err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	// 任务完成，调用回调函数
	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
