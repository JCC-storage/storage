package local

import (
	"fmt"
	"path/filepath"
	"reflect"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

func init() {
	reg.RegisterBuilder[*cdssdk.LocalStorageType](createService, createComponent)
}

func createService(detail stgmod.StorageDetail) (types.StorageService, error) {
	svc := &Service{}

	if detail.Storage.ShardStore != nil {
		local, ok := detail.Storage.ShardStore.(*cdssdk.LocalShardStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shard store type %T for local storage", detail.Storage.ShardStore)
		}

		store, err := NewShardStore(svc, *local)
		if err != nil {
			return nil, err
		}

		svc.ShardStore = store
	}

	if detail.Storage.SharedStore != nil {
		local, ok := detail.Storage.SharedStore.(*cdssdk.LocalSharedStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shared store type %T for local storage", detail.Storage.SharedStore)
		}

		store, err := NewSharedStore(svc, *local)
		if err != nil {
			return nil, err
		}

		svc.SharedStore = store
	}

	return svc, nil
}

func createComponent(detail stgmod.StorageDetail, typ reflect.Type) (any, error) {
	switch typ {
	case reflect2.TypeOf[types.MultipartInitiator]():
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

	case reflect2.TypeOf[types.MultipartUploader]():
		feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](detail)
		if feat == nil {
			return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
		}

		return &MultipartUploader{}, nil
	}

	return nil, fmt.Errorf("unsupported component type %v", typ)
}
