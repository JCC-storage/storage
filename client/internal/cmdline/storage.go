package cmdline

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// StorageCreatePackage 创建一个新的包并上传到指定的存储系统。
// ctx: 命令上下文，提供必要的服务和环境配置。
// bucketID: 存储桶的唯一标识，包将被上传到这个存储桶中。
// name: 新包的名称。
// storageID: 目标存储系统的唯一标识。
// path: 包在存储系统中的路径。
// 返回值: 执行过程中遇到的任何错误。
func StorageCreatePackage(ctx CommandContext, bucketID cdssdk.BucketID, name string, storageID cdssdk.StorageID, path string) error {
	startTime := time.Now()
	defer func() {
		// 打印函数执行时间
		fmt.Printf("%v\n", time.Since(startTime).Seconds())
	}()

	// 开始创建并上传包到存储系统
	hubID, taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageCreatePackage(1, bucketID, name, storageID, path, 0)
	if err != nil {
		return fmt.Errorf("start storage uploading package: %w", err)
	}

	// 循环等待上传完成
	for {
		complete, packageID, err := ctx.Cmdline.Svc.StorageSvc().WaitStorageCreatePackage(hubID, taskID, time.Second*10)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading complete with: %w", err)
			}

			fmt.Printf("%d\n", packageID)
			return nil
		}

		if err != nil {
			return fmt.Errorf("wait uploading: %w", err)
		}
	}
}

// 初始化函数，注册加载包和创建包的命令到命令行解析器。
func init() {
	// 注册创建包命令
	commands.MustAdd(StorageCreatePackage, "stg", "pkg", "new")
}
