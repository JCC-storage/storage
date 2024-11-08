package cmdline

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

// 必须添加的命令函数，用于处理对象上传。
//
// ctx: 命令上下文，提供必要的服务和环境配置。
// packageID: 上传套餐的唯一标识。
// rootPath: 本地文件系统中待上传文件的根目录。
// storageAffinity: 偏好的节点ID列表，上传任务可能会分配到这些节点上。
// 返回值: 执行过程中遇到的任何错误。
var _ = MustAddCmd(func(ctx CommandContext, packageID cdssdk.PackageID, rootPath string, storageAffinity []cdssdk.StorageID) error {
	// 记录函数开始时间，用于计算执行时间。
	startTime := time.Now()
	defer func() {
		// 打印函数执行时间。
		fmt.Printf("%v\n", time.Since(startTime).Seconds())
	}()

	// 模拟或获取用户ID。
	userID := cdssdk.UserID(1)

	// 遍历根目录下所有文件，收集待上传的文件路径。
	var uploadFilePathes []string
	err := filepath.WalkDir(rootPath, func(fname string, fi os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		// 仅添加非目录文件路径。
		if !fi.IsDir() {
			uploadFilePathes = append(uploadFilePathes, fname)
		}

		return nil
	})
	if err != nil {
		// 目录遍历失败处理。
		return fmt.Errorf("open directory %s failed, err: %w", rootPath, err)
	}

	// 根据节点亲和性列表设置首选上传节点。
	var storageAff cdssdk.StorageID
	if len(storageAffinity) > 0 {
		storageAff = storageAffinity[0]
	}

	// 创建上传对象迭代器。
	objIter := iterator.NewUploadingObjectIterator(rootPath, uploadFilePathes)
	// 开始上传任务。
	taskID, err := ctx.Cmdline.Svc.ObjectSvc().StartUploading(userID, packageID, objIter, storageAff)
	if err != nil {
		// 上传任务启动失败处理。
		return fmt.Errorf("update objects to package %d failed, err: %w", packageID, err)
	}

	// 循环等待上传任务完成。
	for {
		// 每5秒检查一次上传状态。
		complete, _, err := ctx.Cmdline.Svc.ObjectSvc().WaitUploading(taskID, time.Second*5)
		if complete {
			// 上传完成，检查是否有错误。
			if err != nil {
				return fmt.Errorf("uploading objects: %w", err)
			}

			return nil
		}

		// 等待过程中发生错误处理。
		if err != nil {
			return fmt.Errorf("wait updating: %w", err)
		}
	}
}, "obj", "upload")
