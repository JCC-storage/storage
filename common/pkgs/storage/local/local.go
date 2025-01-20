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
	types.EmptyBuilder
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

	if b.detail.Storage.PublicStore != nil {
		local, ok := b.detail.Storage.PublicStore.(*cdssdk.LocalPublicStorage)
		if !ok {
			return nil, fmt.Errorf("invalid public store type %T for local storage", b.detail.Storage.PublicStore)
		}

		store, err := NewPublicStore(agt, *local)
		if err != nil {
			return nil, err
		}

		agt.PublicStore = store
	}

	return agt, nil
}

func (b *builder) ShardStoreDesc() types.ShardStoreDesc {
	return &ShardStoreDesc{builder: b}
}

func (b *builder) PublicStoreDesc() types.PublicStoreDesc {
	return &PublicStoreDesc{builder: b}
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

func (b *builder) CreateS2STransfer() (types.S2STransfer, error) {
	feat := utils.FindFeature[*cdssdk.S2STransferFeature](b.detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.S2STransferFeature{})
	}

	return &S2STransfer{
		detail: b.detail,
	}, nil
}
