package local

import (
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type Agent struct {
	Detail      stgmod.StorageDetail
	ShardStore  *ShardStore
	SharedStore *SharedStore
}

func (s *Agent) Start(ch *types.StorageEventChan) {
	if s.ShardStore != nil {
		s.ShardStore.Start(ch)
	}

	if s.SharedStore != nil {
		s.SharedStore.Start(ch)
	}
}

func (s *Agent) Stop() {
	if s.ShardStore != nil {
		s.ShardStore.Stop()
	}

	if s.SharedStore != nil {
		s.SharedStore.Stop()
	}
}

func (s *Agent) Info() stgmod.StorageDetail {
	return s.Detail
}

func (a *Agent) GetShardStore() (types.ShardStore, error) {
	if a.ShardStore == nil {
		return nil, types.ErrUnsupported
	}

	return a.ShardStore, nil
}

func (a *Agent) GetSharedStore() (types.SharedStore, error) {
	if a.SharedStore == nil {
		return nil, types.ErrUnsupported
	}

	return a.SharedStore, nil
}
