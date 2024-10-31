package pool

import (
	"fmt"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/async"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/storages/local"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/types"
)

type ShardStorePool struct {
	stores map[cdssdk.StorageID]*shardStore
	lock   sync.Mutex
}

func New() *ShardStorePool {
	return &ShardStorePool{
		stores: make(map[cdssdk.StorageID]*shardStore),
	}
}

func (p *ShardStorePool) PutNew(stg cdssdk.Storage, config cdssdk.ShardStoreConfig) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	switch confg := config.(type) {
	case *cdssdk.LocalShardStorage:
		if _, ok := p.stores[stg.StorageID]; ok {
			return fmt.Errorf("storage %s already exists", stg.StorageID)
		}

		store, err := local.New(stg, *confg)
		if err != nil {
			return fmt.Errorf("new local shard store: %v", err)
		}

		ch := store.Start()

		p.stores[stg.StorageID] = &shardStore{
			Store:     store,
			EventChan: ch,
		}
		return nil

	default:
		return fmt.Errorf("unsupported shard store type: %T", confg)
	}
}

// 不存在时返回nil
func (p *ShardStorePool) Get(stgID cdssdk.StorageID) types.ShardStore {
	p.lock.Lock()
	defer p.lock.Unlock()

	store, ok := p.stores[stgID]
	if !ok {
		return nil
	}

	return store.Store
}

type shardStore struct {
	Store     types.ShardStore
	EventChan *async.UnboundChannel[types.StoreEvent]
}
