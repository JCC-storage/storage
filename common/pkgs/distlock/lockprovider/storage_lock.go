package lockprovider

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/utils/lo2"
)

const (
	StorageLockPathPrefix = "Storage"
	StorageHubIDPathIndex = 1
	StorageBuzyLock       = "Buzy"
	StorageGCLock         = "GC"
)

type StorageLock struct {
	nodeLocks map[string]*StorageNodeLock
	dummyLock *StorageNodeLock
}

func NewStorageLock() *StorageLock {
	return &StorageLock{
		nodeLocks: make(map[string]*StorageNodeLock),
		dummyLock: NewStorageNodeLock(),
	}
}

// CanLock 判断这个锁能否锁定成功
func (l *StorageLock) CanLock(lock distlock.Lock) error {
	nodeLock, ok := l.nodeLocks[lock.Path[StorageHubIDPathIndex]]
	if !ok {
		// 不能直接返回nil，因为如果锁数据的格式不对，也不能获取锁。
		// 这里使用一个空Provider来进行检查。
		return l.dummyLock.CanLock(lock)
	}

	return nodeLock.CanLock(lock)
}

// 锁定。在内部可以不用判断能否加锁，外部需要保证调用此函数前调用了CanLock进行检查
func (l *StorageLock) Lock(reqID string, lock distlock.Lock) error {
	hubID := lock.Path[StorageHubIDPathIndex]

	nodeLock, ok := l.nodeLocks[hubID]
	if !ok {
		nodeLock = NewStorageNodeLock()
		l.nodeLocks[hubID] = nodeLock
	}

	return nodeLock.Lock(reqID, lock)
}

// 解锁
func (l *StorageLock) Unlock(reqID string, lock distlock.Lock) error {
	hubID := lock.Path[StorageHubIDPathIndex]

	nodeLock, ok := l.nodeLocks[hubID]
	if !ok {
		return nil
	}

	return nodeLock.Unlock(reqID, lock)
}

// GetTargetString 将锁对象序列化为字符串，方便存储到ETCD
func (l *StorageLock) GetTargetString(target any) (string, error) {
	tar := target.(StringLockTarget)
	return StringLockTargetToString(&tar)
}

// ParseTargetString 解析字符串格式的锁对象数据
func (l *StorageLock) ParseTargetString(targetStr string) (any, error) {
	return StringLockTargetFromString(targetStr)
}

// Clear 清除内部所有状态
func (l *StorageLock) Clear() {
	l.nodeLocks = make(map[string]*StorageNodeLock)
}

type StorageNodeLock struct {
	buzyReqIDs []string
	gcReqIDs   []string

	lockCompatibilityTable *LockCompatibilityTable
}

func NewStorageNodeLock() *StorageNodeLock {
	compTable := &LockCompatibilityTable{}

	StorageLock := StorageNodeLock{
		lockCompatibilityTable: compTable,
	}

	compTable.
		Column(StorageBuzyLock, func() bool { return len(StorageLock.buzyReqIDs) > 0 }).
		Column(StorageGCLock, func() bool { return len(StorageLock.gcReqIDs) > 0 })

	comp := LockCompatible()
	uncp := LockUncompatible()

	compTable.MustRow(comp, uncp)
	compTable.MustRow(uncp, comp)

	return &StorageLock
}

// CanLock 判断这个锁能否锁定成功
func (l *StorageNodeLock) CanLock(lock distlock.Lock) error {
	return l.lockCompatibilityTable.Test(lock)
}

// 锁定
func (l *StorageNodeLock) Lock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case StorageBuzyLock:
		l.buzyReqIDs = append(l.buzyReqIDs, reqID)
	case StorageGCLock:
		l.gcReqIDs = append(l.gcReqIDs, reqID)
	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}

// 解锁
func (l *StorageNodeLock) Unlock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case StorageBuzyLock:
		l.buzyReqIDs = lo2.Remove(l.buzyReqIDs, reqID)
	case StorageGCLock:
		l.gcReqIDs = lo2.Remove(l.gcReqIDs, reqID)
	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}
