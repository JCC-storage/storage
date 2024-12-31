package s3

import (
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type Agent struct {
	Detail     stgmod.StorageDetail
	ShardStore *ShardStore
}

func (s *Agent) Start(ch *types.StorageEventChan) {
	if s.ShardStore != nil {
		s.ShardStore.Start(ch)
	}
}

func (a *Agent) Stop() {
	if a.ShardStore != nil {
		a.ShardStore.Stop()
	}
}

func (a *Agent) Info() stgmod.StorageDetail {
	return a.Detail
}

func (a *Agent) GetShardStore() (types.ShardStore, error) {
	if a.ShardStore == nil {
		return nil, types.ErrUnsupported
	}

	return a.ShardStore, nil
}

func (a *Agent) GetSharedStore() (types.SharedStore, error) {
	return nil, types.ErrUnsupported
}
