package local

import (
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type agent struct {
	Detail      stgmod.StorageDetail
	ShardStore  *ShardStore
	PublicStore *PublicStore
}

func (s *agent) Start(ch *types.StorageEventChan) {
	if s.ShardStore != nil {
		s.ShardStore.Start(ch)
	}

	if s.PublicStore != nil {
		s.PublicStore.Start(ch)
	}
}

func (s *agent) Stop() {
	if s.ShardStore != nil {
		s.ShardStore.Stop()
	}

	if s.PublicStore != nil {
		s.PublicStore.Stop()
	}
}

func (s *agent) Info() stgmod.StorageDetail {
	return s.Detail
}

func (a *agent) GetShardStore() (types.ShardStore, error) {
	if a.ShardStore == nil {
		return nil, types.ErrUnsupported
	}

	return a.ShardStore, nil
}

func (a *agent) GetPublicStore() (types.PublicStore, error) {
	if a.PublicStore == nil {
		return nil, types.ErrUnsupported
	}

	return a.PublicStore, nil
}
