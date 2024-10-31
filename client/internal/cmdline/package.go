package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

// PackageListBucketPackages 列出指定存储桶中的所有包裹。
//
// 参数:
//
//	ctx - 命令上下文。
//	bucketID - 存储桶ID。
//
// 返回值:
//
//	error - 操作过程中发生的任何错误。
func PackageListBucketPackages(ctx CommandContext, bucketID cdssdk.BucketID) error {
	userID := cdssdk.UserID(1)

	packages, err := ctx.Cmdline.Svc.BucketSvc().GetBucketPackages(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d packages in bucket %d for user %d:\n", len(packages), bucketID, userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "BucketID", "State"})

	for _, obj := range packages {
		tb.AppendRow(table.Row{obj.PackageID, obj.Name, obj.BucketID, obj.State})
	}

	fmt.Println(tb.Render())
	return nil
}

// PackageDownloadPackage 下载指定包裹的所有文件到本地目录。
//
// 参数:
//
//	ctx - 命令上下文。
//	packageID - 包裹ID。
//	outputDir - 输出目录路径。
//
// 返回值:
//
//	error - 操作过程中发生的任何错误。
func PackageDownloadPackage(ctx CommandContext, packageID cdssdk.PackageID, outputDir string) error {
	startTime := time.Now()
	defer func() {
		fmt.Printf("%v\n", time.Since(startTime).Seconds())
	}()

	userID := cdssdk.UserID(1)

	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create output directory %s failed, err: %w", outputDir, err)
	}

	// 初始化文件下载迭代器
	objIter, err := ctx.Cmdline.Svc.PackageSvc().DownloadPackage(userID, packageID)
	if err != nil {
		return fmt.Errorf("download object failed, err: %w", err)
	}
	defer objIter.Close()

	madeDirs := make(map[string]bool)

	for {
		objInfo, err := objIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			return err
		}

		err = func() error {
			defer objInfo.File.Close()

			fullPath := filepath.Join(outputDir, objInfo.Object.Path)

			dirPath := filepath.Dir(fullPath)
			if !madeDirs[dirPath] {
				if err := os.MkdirAll(dirPath, 0755); err != nil {
					return fmt.Errorf("creating object dir: %w", err)
				}
				madeDirs[dirPath] = true
			}

			outputFile, err := os.Create(fullPath)
			if err != nil {
				return fmt.Errorf("creating object file: %w", err)
			}
			defer outputFile.Close()

			_, err = io.Copy(outputFile, objInfo.File)
			if err != nil {
				return fmt.Errorf("copy object data to local file failed, err: %w", err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

// PackageCreatePackage 在指定存储桶中创建新包裹。
//
// 参数:
//
//	ctx - 命令上下文。
//	bucketID - 存储桶ID。
//	name - 包裹名称。
//
// 返回值:
//
//	error - 操作过程中发生的任何错误。
func PackageCreatePackage(ctx CommandContext, bucketID cdssdk.BucketID, name string) error {
	userID := cdssdk.UserID(1)

	pkgID, err := ctx.Cmdline.Svc.PackageSvc().Create(userID, bucketID, name)
	if err != nil {
		return err
	}

	fmt.Printf("%v\n", pkgID)
	return nil
}

// PackageDeletePackage 删除指定的包裹。
//
// 参数:
//
//	ctx - 命令上下文。
//	packageID - 包裹ID。
//
// 返回值:
//
//	error - 操作过程中发生的任何错误。
func PackageDeletePackage(ctx CommandContext, packageID cdssdk.PackageID) error {
	userID := cdssdk.UserID(1)
	err := ctx.Cmdline.Svc.PackageSvc().DeletePackage(userID, packageID)
	if err != nil {
		return fmt.Errorf("delete package %d failed, err: %w", packageID, err)
	}
	return nil
}

// PackageGetCachedStorages 获取指定包裹的缓存节点信息。
//
// 参数:
//
//	ctx - 命令上下文。
//	packageID - 包裹ID。
//
// 返回值:
//
//	error - 操作过程中发生的任何错误。
func PackageGetCachedStorages(ctx CommandContext, packageID cdssdk.PackageID) error {
	userID := cdssdk.UserID(1)
	resp, err := ctx.Cmdline.Svc.PackageSvc().GetCachedStorages(userID, packageID)
	fmt.Printf("resp: %v\n", resp)
	if err != nil {
		return fmt.Errorf("get package %d cached nodes failed, err: %w", packageID, err)
	}
	return nil
}

// PackageGetLoadedStorages 获取指定包裹的已加载节点信息。
//
// 参数:
//
//	ctx - 命令上下文。
//	packageID - 包裹ID。
//
// 返回值:
//
//	error - 操作过程中发生的任何错误。
func PackageGetLoadedStorages(ctx CommandContext, packageID cdssdk.PackageID) error {
	userID := cdssdk.UserID(1)
	nodeIDs, err := ctx.Cmdline.Svc.PackageSvc().GetLoadedStorages(userID, packageID)
	fmt.Printf("nodeIDs: %v\n", nodeIDs)
	if err != nil {
		return fmt.Errorf("get package %d loaded nodes failed, err: %w", packageID, err)
	}
	return nil
}

// 初始化命令行工具的包相关命令。
func init() {
	commands.MustAdd(PackageListBucketPackages, "pkg", "ls")

	commands.MustAdd(PackageDownloadPackage, "pkg", "get")

	commands.MustAdd(PackageCreatePackage, "pkg", "new")

	commands.MustAdd(PackageDeletePackage, "pkg", "delete")

	// 查询package缓存到哪些节点
	commands.MustAdd(PackageGetCachedStorages, "pkg", "cached")

	// 查询package调度到哪些节点
	commands.MustAdd(PackageGetLoadedStorages, "pkg", "loaded")
}
