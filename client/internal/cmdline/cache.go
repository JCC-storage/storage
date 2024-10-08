package cmdline

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func CacheMovePackage(ctx CommandContext, packageID cdssdk.PackageID, nodeID cdssdk.NodeID) error {
	startTime := time.Now()
	defer func() {
		fmt.Printf("%v\n", time.Since(startTime).Seconds())
	}()

	taskID, err := ctx.Cmdline.Svc.CacheSvc().StartCacheMovePackage(1, packageID, nodeID)
	if err != nil {
		return fmt.Errorf("start cache moving package: %w", err)
	}

	for {
		complete, err := ctx.Cmdline.Svc.CacheSvc().WaitCacheMovePackage(nodeID, taskID, time.Second*10)
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

func CacheRemovePackage(ctx CommandContext, packageID cdssdk.PackageID, nodeID cdssdk.NodeID) error {
	return ctx.Cmdline.Svc.CacheSvc().CacheRemovePackage(packageID, nodeID)
}

func init() {
	commands.Add(CacheMovePackage, "cache", "move")

	commands.Add(CacheRemovePackage, "cache", "remove")
}
