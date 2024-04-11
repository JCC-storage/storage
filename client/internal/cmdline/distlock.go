package cmdline

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

// DistLockLock 尝试获取分布式锁。
// ctx: 命令上下文，包含执行命令所需的服务和配置。
// lockData: 锁数据数组，每个元素包含锁的路径、名称和目标。
// 返回值: 获取锁失败时返回错误。
func DistLockLock(ctx CommandContext, lockData []string) error {
	req := distlock.LockRequest{}

	// 解析锁数据，填充请求结构体。
	for _, lock := range lockData {
		l, err := parseOneLock(lock)
		if err != nil {
			return fmt.Errorf("parse lock data %s failed, err: %w", lock, err)
		}

		req.Locks = append(req.Locks, l)
	}

	// 请求分布式锁。
	reqID, err := ctx.Cmdline.Svc.DistLock.Acquire(req)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}

	fmt.Printf("%s\n", reqID)

	return nil
}

// parseOneLock 解析单个锁数据。
// lockData: 待解析的锁数据，格式为"路径/名称@目标字符串"。
// 返回值: 解析得到的锁对象和可能的错误。
func parseOneLock(lockData string) (distlock.Lock, error) {
	var lock distlock.Lock

	// 解析锁的路径、名称和目标。
	fullPathAndTarget := strings.Split(lockData, "@")
	if len(fullPathAndTarget) != 2 {
		return lock, fmt.Errorf("lock data must contains lock path, name and target")
	}

	pathAndName := strings.Split(fullPathAndTarget[0], "/")
	if len(pathAndName) < 2 {
		return lock, fmt.Errorf("lock data must contains lock path, name and target")
	}

	lock.Path = pathAndName[0 : len(pathAndName)-1]
	lock.Name = pathAndName[len(pathAndName)-1]

	// 解析目标字符串。
	target := lockprovider.NewStringLockTarget()
	comps := strings.Split(fullPathAndTarget[1], "/")
	for _, comp := range comps {
		target.Add(lo.Map(strings.Split(comp, "."), func(str string, index int) any { return str })...)
	}

	lock.Target = *target

	return lock, nil
}

// DistLockUnlock 释放分布式锁。
// ctx: 命令上下文。
// reqID: 请求ID，对应获取锁时返回的ID。
// 返回值: 释放锁失败时返回错误。
func DistLockUnlock(ctx CommandContext, reqID string) error {
	ctx.Cmdline.Svc.DistLock.Release(reqID)
	return nil
}

// 初始化命令行工具，注册分布式锁相关命令。
func init() {
	commands.MustAdd(DistLockLock, "distlock", "lock")

	commands.MustAdd(DistLockUnlock, "distlock", "unlock")
}
