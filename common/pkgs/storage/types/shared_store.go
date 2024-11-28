package types

import (
	"io"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type SharedStore interface {
	Start(ch *StorageEventChan)
	Stop()
	// 写入一个文件到Package的调度目录下，返回值为文件路径：userID/pkgID/path
	WritePackageObject(userID cdssdk.UserID, pkgID cdssdk.PackageID, path string, stream io.Reader) (string, error)
	// 获取所有已加载的Package信息
	ListLoadedPackages() ([]stgmod.LoadedPackageID, error)
	// 垃圾回收，删除过期的Package
	PackageGC(avaiables []stgmod.LoadedPackageID) error
}
