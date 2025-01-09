package factory

import (
	"fmt"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type EmptyBuilder struct {
	detail stgmod.StorageDetail
}

// 创建一个在MasterHub上长期运行的存储服务
func (b *EmptyBuilder) CreateAgent() (types.StorageAgent, error) {
	return nil, fmt.Errorf("create agent for %T: %w", b.detail.Storage.Type, types.ErrUnsupported)
}

// 创建一个分片上传组件
func (b *EmptyBuilder) CreateMultiparter() (types.Multiparter, error) {
	return nil, fmt.Errorf("create multipart initiator for %T: %w", b.detail.Storage.Type, types.ErrUnsupported)
}
