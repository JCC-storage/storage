package pool

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/shardstore/types"
)

type ShardStorePool struct {
}

func (p *ShardStorePool) Get(stgID cdssdk.StorageID) (types.ShardStore, error) {

}
