package mashup

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func init() {
	reg.RegisterBuilder[*cdssdk.MashupStorageType](func(detail stgmod.StorageDetail) types.StorageBuilder {
		return &builder{
			detail: detail,
		}
	})
}

type builder struct {
	detail stgmod.StorageDetail
}

func (b *builder) CreateAgent() (types.StorageAgent, error) {
	stgType := b.detail.Storage.Type.(*cdssdk.MashupStorageType)
	detail := b.detail
	detail.Storage.Type = stgType.Agent

	blder := factory.GetBuilder(detail)
	return blder.CreateAgent()
}

func (b *builder) ShardStoreDesc() types.ShardStoreDesc {
	stgType := b.detail.Storage.Type.(*cdssdk.MashupStorageType)
	detail := b.detail
	detail.Storage.Type = stgType.Agent

	blder := factory.GetBuilder(detail)
	return blder.ShardStoreDesc()
}

func (b *builder) SharedStoreDesc() types.SharedStoreDesc {
	stgType := b.detail.Storage.Type.(*cdssdk.MashupStorageType)
	detail := b.detail
	detail.Storage.Type = stgType.Agent

	blder := factory.GetBuilder(detail)
	return blder.SharedStoreDesc()
}

func (b *builder) CreateMultiparter() (types.Multiparter, error) {
	stgType := b.detail.Storage.Type.(*cdssdk.MashupStorageType)
	detail := b.detail
	detail.Storage.Type = stgType.Feature

	blder := factory.GetBuilder(detail)
	return blder.CreateMultiparter()
}

func (b *builder) CreateS2STransfer() (types.S2STransfer, error) {
	stgType := b.detail.Storage.Type.(*cdssdk.MashupStorageType)
	detail := b.detail
	detail.Storage.Type = stgType.Feature

	blder := factory.GetBuilder(detail)
	return blder.CreateS2STransfer()
}

func (b *builder) CreateECMultiplier() (types.ECMultiplier, error) {
	stgType := b.detail.Storage.Type.(*cdssdk.MashupStorageType)
	detail := b.detail
	detail.Storage.Type = stgType.Feature

	blder := factory.GetBuilder(detail)
	return blder.CreateECMultiplier()
}
