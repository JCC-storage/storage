package reqbuilder

import (
	"strconv"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type ShardStoreLockReqBuilder struct {
	*LockRequestBuilder
}

func (b *LockRequestBuilder) Shard() *ShardStoreLockReqBuilder {
	return &ShardStoreLockReqBuilder{LockRequestBuilder: b}
}
func (b *ShardStoreLockReqBuilder) Buzy(stgID cdssdk.StorageID) *ShardStoreLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(stgID),
		Name:   lockprovider.IPFSBuzyLock,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *ShardStoreLockReqBuilder) GC(stgID cdssdk.StorageID) *ShardStoreLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(stgID),
		Name:   lockprovider.IPFSGCLock,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *ShardStoreLockReqBuilder) makePath(nodeID cdssdk.StorageID) []string {
	return []string{lockprovider.IPFSLockPathPrefix, strconv.FormatInt(int64(nodeID), 10)}
}
