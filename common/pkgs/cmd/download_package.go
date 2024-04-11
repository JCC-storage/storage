package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// 下载包结构体，存储用户ID、包ID和输出路径。
type DownloadPackage struct {
	userID     cdssdk.UserID
	packageID  cdssdk.PackageID
	outputPath string
}

// 下载包执行上下文，包含分布式锁服务。
type DownloadPackageContext struct {
	Distlock *distlock.Service
}

// 新建一个下载包实例。
// userID: 用户标识。
// packageID: 包标识。
// outputPath: 输出路径。
func NewDownloadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, outputPath string) *DownloadPackage {
	return &DownloadPackage{
		userID:     userID,
		packageID:  packageID,
		outputPath: outputPath,
	}
}

// 执行下载包操作。
// ctx: 下载包执行上下文。
// 返回值: 执行过程中可能出现的错误。
func (t *DownloadPackage) Execute(ctx *DownloadPackageContext) error {
	// 获取协调器MQ客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli) // 确保释放客户端资源

	// 获取包内对象详情
	getObjectDetails, err := coorCli.GetPackageObjectDetails(coormq.NewGetPackageObjectDetails(t.packageID))
	if err != nil {
		return fmt.Errorf("getting package object details: %w", err)
	}

	// 创建下载对象迭代器
	objIter := iterator.NewDownloadObjectIterator(getObjectDetails.Objects, &iterator.DownloadContext{
		Distlock: ctx.Distlock,
	})
	defer objIter.Close() // 确保迭代器关闭

	// 写入对象数据到本地
	return t.writeObjects(objIter)
}

// 将下载的对象写入本地文件系统。
// objIter: 下载中的对象迭代器。
// 返回值: 写入过程中可能出现的错误。
func (t *DownloadPackage) writeObjects(objIter iterator.DownloadingObjectIterator) error {
	for {
		objInfo, err := objIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break // 没有更多对象时结束循环
		}
		if err != nil {
			return err
		}

		err = func() error {
			defer objInfo.File.Close() // 确保文件资源被释放

			fullPath := filepath.Join(t.outputPath, objInfo.Object.Path) // 计算文件完整路径

			dirPath := filepath.Dir(fullPath)                  // 获取文件所在目录路径
			if err := os.MkdirAll(dirPath, 0755); err != nil { // 创建目录，如果不存在
				return fmt.Errorf("creating object dir: %w", err)
			}

			outputFile, err := os.Create(fullPath) // 创建本地文件
			if err != nil {
				return fmt.Errorf("creating object file: %w", err)
			}
			defer outputFile.Close() // 确保文件关闭

			_, err = io.Copy(outputFile, objInfo.File) // 将对象数据写入本地文件
			if err != nil {
				return fmt.Errorf("copy object data to local file failed, err: %w", err)
			}

			return nil
		}()
		if err != nil {
			return err // 如果写入过程中出现错误，返回该错误
		}
	}

	return nil // 没有错误，返回nil
}
