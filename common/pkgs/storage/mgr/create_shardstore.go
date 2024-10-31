package mgr

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/storages/local"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func createShardStore(detail stgmod.StorageDetail, ch *types.StorageEventChan, stg *storage) error {
	switch confg := detail.Shard.Config.(type) {
	case *cdssdk.LocalShardStorage:
		store, err := local.New(detail.Storage, *confg)
		if err != nil {
			return fmt.Errorf("new local shard store: %v", err)
		}

		store.Start(ch)
		stg.Shard = store
		return nil

	default:
		return fmt.Errorf("unsupported shard store type: %T", confg)
	}
}
