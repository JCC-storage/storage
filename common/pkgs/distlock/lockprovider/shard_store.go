package lockprovider

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/utils/lo2"
)

const (
	ShardStoreLockPathPrefix     = "ShardStore"
	ShardStoreStorageIDPathIndex = 1
	ShardStoreBuzyLock           = "Buzy"
	ShardStoreGCLock             = "GC"
)

type ShardStoreLock struct {
	stgLocks  map[string]*ShardStoreStorageLock
	dummyLock *ShardStoreStorageLock
}

func NewShardStoreLock() *ShardStoreLock {
	return &ShardStoreLock{
		stgLocks:  make(map[string]*ShardStoreStorageLock),
		dummyLock: NewShardStoreStorageLock(),
	}
}

// CanLock 判断这个锁能否锁定成功
func (l *ShardStoreLock) CanLock(lock distlock.Lock) error {
	nodeLock, ok := l.stgLocks[lock.Path[ShardStoreStorageIDPathIndex]]
	if !ok {
		// 不能直接返回nil，因为如果锁数据的格式不对，也不能获取锁。
		// 这里使用一个空Provider来进行检查。
		return l.dummyLock.CanLock(lock)
	}

	return nodeLock.CanLock(lock)
}

// 锁定。在内部可以不用判断能否加锁，外部需要保证调用此函数前调用了CanLock进行检查
func (l *ShardStoreLock) Lock(reqID string, lock distlock.Lock) error {
	stgID := lock.Path[ShardStoreStorageIDPathIndex]

	nodeLock, ok := l.stgLocks[stgID]
	if !ok {
		nodeLock = NewShardStoreStorageLock()
		l.stgLocks[stgID] = nodeLock
	}

	return nodeLock.Lock(reqID, lock)
}

// 解锁
func (l *ShardStoreLock) Unlock(reqID string, lock distlock.Lock) error {
	stgID := lock.Path[ShardStoreStorageIDPathIndex]

	nodeLock, ok := l.stgLocks[stgID]
	if !ok {
		return nil
	}

	return nodeLock.Unlock(reqID, lock)
}

// GetTargetString 将锁对象序列化为字符串，方便存储到ETCD
func (l *ShardStoreLock) GetTargetString(target any) (string, error) {
	tar := target.(StringLockTarget)
	return StringLockTargetToString(&tar)
}

// ParseTargetString 解析字符串格式的锁对象数据
func (l *ShardStoreLock) ParseTargetString(targetStr string) (any, error) {
	return StringLockTargetFromString(targetStr)
}

// Clear 清除内部所有状态
func (l *ShardStoreLock) Clear() {
	l.stgLocks = make(map[string]*ShardStoreStorageLock)
}

type ShardStoreStorageLock struct {
	buzyReqIDs []string
	gcReqIDs   []string

	lockCompatibilityTable *LockCompatibilityTable
}

func NewShardStoreStorageLock() *ShardStoreStorageLock {
	compTable := &LockCompatibilityTable{}

	sdLock := ShardStoreStorageLock{
		lockCompatibilityTable: compTable,
	}

	compTable.
		Column(ShardStoreBuzyLock, func() bool { return len(sdLock.buzyReqIDs) > 0 }).
		Column(ShardStoreGCLock, func() bool { return len(sdLock.gcReqIDs) > 0 })

	comp := LockCompatible()
	uncp := LockUncompatible()

	compTable.MustRow(comp, uncp)
	compTable.MustRow(uncp, comp)

	return &sdLock
}

// CanLock 判断这个锁能否锁定成功
func (l *ShardStoreStorageLock) CanLock(lock distlock.Lock) error {
	return l.lockCompatibilityTable.Test(lock)
}

// 锁定
func (l *ShardStoreStorageLock) Lock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case ShardStoreBuzyLock:
		l.buzyReqIDs = append(l.buzyReqIDs, reqID)
	case ShardStoreGCLock:
		l.gcReqIDs = append(l.gcReqIDs, reqID)
	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}

// 解锁
func (l *ShardStoreStorageLock) Unlock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case ShardStoreBuzyLock:
		l.buzyReqIDs = lo2.Remove(l.buzyReqIDs, reqID)
	case ShardStoreGCLock:
		l.gcReqIDs = lo2.Remove(l.gcReqIDs, reqID)
	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}
