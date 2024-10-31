package cmdline

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func CacheMovePackage(ctx CommandContext, packageID cdssdk.PackageID, stgID cdssdk.StorageID) error {
	startTime := time.Now()
	defer func() {
		fmt.Printf("%v\n", time.Since(startTime).Seconds())
	}()

	hubID, taskID, err := ctx.Cmdline.Svc.CacheSvc().StartCacheMovePackage(1, packageID, stgID)
	if err != nil {
		return fmt.Errorf("start cache moving package: %w", err)
	}

	for {
		complete, err := ctx.Cmdline.Svc.CacheSvc().WaitCacheMovePackage(hubID, taskID, time.Second*10)
		if complete {
			if err != nil {
				return fmt.Errorf("moving complete with: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("wait moving: %w", err)
		}
	}
}

func CacheRemovePackage(ctx CommandContext, packageID cdssdk.PackageID, stgID cdssdk.StorageID) error {
	return ctx.Cmdline.Svc.CacheSvc().CacheRemovePackage(packageID, stgID)
}

func init() {
	commands.Add(CacheMovePackage, "cache", "move")

	commands.Add(CacheRemovePackage, "cache", "remove")
}
