package cmdline

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
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

	// 根据节点亲和性列表设置首选上传节点。
	var storageAff cdssdk.StorageID
	if len(storageAffinity) > 0 {
		storageAff = storageAffinity[0]
	}

	up, err := ctx.Cmdline.Svc.Uploader.BeginUpdate(userID, packageID, storageAff, nil, nil)
	if err != nil {
		return fmt.Errorf("begin updating package: %w", err)
	}
	defer up.Abort()

	err = filepath.WalkDir(rootPath, func(fname string, fi os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if fi.IsDir() {
			return nil
		}

		info, err := fi.Info()
		if err != nil {
			return err
		}
		file, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer file.Close()

		return up.Upload(fname, info.Size(), file)
	})
	if err != nil {
		return err
	}

	_, err = up.Commit()
	if err != nil {
		return fmt.Errorf("commit updating package: %w", err)
	}

	return nil

}, "obj", "upload")
