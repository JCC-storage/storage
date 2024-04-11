package cmdline

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// CacheMovePackage 移动缓存包到指定节点。
// ctx: 命令上下文环境。
// packageID: 待移动的包ID。
// nodeID: 目标节点ID。
// 返回值: 移动成功返回nil，失败返回error。
func CacheMovePackage(ctx CommandContext, packageID cdssdk.PackageID, nodeID cdssdk.NodeID) error {
	startTime := time.Now()
	defer func() {
		// 打印函数执行时间
		fmt.Printf("%v\n", time.Since(startTime).Seconds())
	}()

	// 开始移动缓存包任务
	taskID, err := ctx.Cmdline.Svc.CacheSvc().StartCacheMovePackage(1, packageID, nodeID)
	if err != nil {
		return fmt.Errorf("start cache moving package: %w", err)
	}

	// 循环等待缓存包移动完成
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

// CacheRemovePackage 从缓存中移除指定的包。
// ctx: 命令上下文环境。
// packageID: 待移除的包ID。
// nodeID: 缓存节点ID。
// 返回值: 移除成功返回nil，失败返回error。
func CacheRemovePackage(ctx CommandContext, packageID cdssdk.PackageID, nodeID cdssdk.NodeID) error {
	return ctx.Cmdline.Svc.CacheSvc().CacheRemovePackage(packageID, nodeID)
}

// 初始化命令列表
func init() {
	// 添加移动缓存包命令
	commands.Add(CacheMovePackage, "cache", "move")

	// 添加移除缓存包命令
	commands.Add(CacheRemovePackage, "cache", "remove")
}
