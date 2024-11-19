package utils

import (
	"fmt"
	"path/filepath"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func MakeLoadedPackagePath(userID cdssdk.UserID, packageID cdssdk.PackageID) string {
	return filepath.Join(fmt.Sprintf("%v", userID), fmt.Sprintf("%v", packageID))
}
