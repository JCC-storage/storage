package utils

import (
	"fmt"
	"path/filepath"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

func MakeLoadedPackagePath(userID cdssdk.UserID, packageID cdssdk.PackageID) string {
	return filepath.Join(fmt.Sprintf("%v", userID), fmt.Sprintf("%v", packageID))
}

func FindFeature[T cdssdk.StorageFeature](detail stgmod.StorageDetail) T {
	for _, f := range detail.Storage.Features {
		f2, ok := f.(T)
		if ok {
			return f2
		}
	}

	var def T
	return def
}
