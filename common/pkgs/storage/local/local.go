package local

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

func init() {
	reg.RegisterBuilder[*cdssdk.LocalStorageType](func(detail stgmod.StorageDetail) types.StorageBuilder {
		return &builder{
			detail: detail,
		}
	})
}

type builder struct {
	detail stgmod.StorageDetail
}

func (b *builder) CreateAgent() (types.StorageAgent, error) {
	agt := &agent{
		Detail: b.detail,
	}

	if b.detail.Storage.ShardStore != nil {
		local, ok := b.detail.Storage.ShardStore.(*cdssdk.LocalShardStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shard store type %T for local storage", b.detail.Storage.ShardStore)
		}

		store, err := NewShardStore(agt, *local)
		if err != nil {
			return nil, err
		}

		agt.ShardStore = store
	}

	if b.detail.Storage.SharedStore != nil {
		local, ok := b.detail.Storage.SharedStore.(*cdssdk.LocalSharedStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shared store type %T for local storage", b.detail.Storage.SharedStore)
		}

		store, err := NewSharedStore(agt, *local)
		if err != nil {
			return nil, err
		}

		agt.SharedStore = store
	}

	return agt, nil
}

func (b *builder) ShardStoreDesc() types.ShardStoreDesc {
	return &ShardStoreDesc{builder: b}
}

func (b *builder) SharedStoreDesc() types.SharedStoreDesc {
	return &SharedStoreDesc{builder: b}
}

func (b *builder) CreateMultiparter() (types.Multiparter, error) {
	feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](b.detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
	}

	return &Multiparter{
		feat: feat,
	}, nil
}
