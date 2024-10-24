package pool

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/types"
)

type ShardStorePool struct {
}

func New() *ShardStorePool {

}

func (p *ShardStorePool) PutNew(stg cdssdk.Storage, config cdssdk.ShardStoreConfig) error {

}

func (p *ShardStorePool) Get(stgID cdssdk.StorageID) (types.ShardStore, error) {

}
