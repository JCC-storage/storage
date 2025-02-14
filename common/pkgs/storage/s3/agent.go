package s3

import (
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type Agent struct {
	Detail      stgmod.StorageDetail
	ShardStore  *ShardStore
	PublicStore *PublicStore
}

func (s *Agent) Start(ch *types.StorageEventChan) {
	if s.ShardStore != nil {
		s.ShardStore.Start(ch)
	}

	if s.PublicStore != nil {
		s.PublicStore.Start(ch)
	}
}

func (a *Agent) Stop() {
	if a.ShardStore != nil {
		a.ShardStore.Stop()
	}

	if a.PublicStore != nil {
		a.PublicStore.Stop()
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

func (a *Agent) GetPublicStore() (types.PublicStore, error) {
	if a.PublicStore == nil {
		return nil, types.ErrUnsupported
	}

	return a.PublicStore, nil
}
