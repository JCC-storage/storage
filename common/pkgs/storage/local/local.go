package local

import (
	"fmt"
	"reflect"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func init() {
	factory.RegisterBuilder[*cdssdk.LocalStorageType](createService, createComponent)
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
	return nil, types.ErrUnsupportedComponent
}
