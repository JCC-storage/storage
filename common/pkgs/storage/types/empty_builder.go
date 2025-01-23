package types

import (
	"fmt"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type EmptyBuilder struct {
	Detail stgmod.StorageDetail
}

// 创建一个在MasterHub上长期运行的存储服务
func (b *EmptyBuilder) CreateAgent() (StorageAgent, error) {
	return nil, fmt.Errorf("create agent for %T: %w", b.Detail.Storage.Type, ErrUnsupported)
}

func (b *EmptyBuilder) ShardStoreDesc() ShardStoreDesc {
	return &EmptyShardStoreDesc{}
}

func (b *EmptyBuilder) PublicStoreDesc() PublicStoreDesc {
	return &EmptyPublicStoreDesc{}
}

// 创建一个分片上传组件
func (b *EmptyBuilder) CreateMultiparter() (Multiparter, error) {
	return nil, fmt.Errorf("create multipart initiator for %T: %w", b.Detail.Storage.Type, ErrUnsupported)
}

func (b *EmptyBuilder) CreateS2STransfer() (S2STransfer, error) {
	return nil, fmt.Errorf("create s2s transfer for %T: %w", b.Detail.Storage.Type, ErrUnsupported)
}

func (b *EmptyBuilder) CreateECMultiplier() (ECMultiplier, error) {
	return nil, fmt.Errorf("create ec multiplier for %T: %w", b.Detail.Storage.Type, ErrUnsupported)
}

type EmptyShardStoreDesc struct {
}

func (d *EmptyShardStoreDesc) Enabled() bool {
	return false
}

func (d *EmptyShardStoreDesc) HasBypassWrite() bool {
	return false
}

func (d *EmptyShardStoreDesc) HasBypassRead() bool {
	return false
}

func (d *EmptyShardStoreDesc) HasBypassHTTPRead() bool {
	return false
}

type EmptyPublicStoreDesc struct {
}

func (d *EmptyPublicStoreDesc) Enabled() bool {
	return false
}

func (d *EmptyPublicStoreDesc) HasBypassWrite() bool {
	return false
}
