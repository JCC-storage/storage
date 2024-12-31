package local

import (
	"fmt"
	"path/filepath"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

func init() {
	reg.RegisterBuilder[*cdssdk.LocalStorageType](&builder{})
}

type builder struct{}

func (b *builder) CreateAgent(detail stgmod.StorageDetail) (types.StorageAgent, error) {
	agt := &Agent{
		Detail: detail,
	}

	if detail.Storage.ShardStore != nil {
		local, ok := detail.Storage.ShardStore.(*cdssdk.LocalShardStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shard store type %T for local storage", detail.Storage.ShardStore)
		}

		store, err := NewShardStore(agt, *local)
		if err != nil {
			return nil, err
		}

		agt.ShardStore = store
	}

	if detail.Storage.SharedStore != nil {
		local, ok := detail.Storage.SharedStore.(*cdssdk.LocalSharedStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shared store type %T for local storage", detail.Storage.SharedStore)
		}

		store, err := NewSharedStore(agt, *local)
		if err != nil {
			return nil, err
		}

		agt.SharedStore = store
	}

	return agt, nil
}

func (b *builder) CreateMultipartInitiator(detail stgmod.StorageDetail) (types.MultipartInitiator, error) {
	feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
	}

	absTempDir, err := filepath.Abs(feat.TempDir)
	if err != nil {
		return nil, fmt.Errorf("get abs temp dir %v: %v", feat.TempDir, err)
	}

	return &MultipartInitiator{
		absTempDir: absTempDir,
	}, nil
}

func (b *builder) CreateMultipartUploader(detail stgmod.StorageDetail) (types.MultipartUploader, error) {
	feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
	}

	return &MultipartUploader{}, nil
}
