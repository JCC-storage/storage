package s3

import (
	"reflect"

	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type Service struct {
	Detail     stgmod.StorageDetail
	ShardStore *ShardStore
}

func (s *Service) Info() stgmod.StorageDetail {
	return s.Detail
}

func (s *Service) GetComponent(typ reflect.Type) (any, error) {
	switch typ {
	case reflect2.TypeOf[types.ShardStore]():
		if s.ShardStore == nil {
			return nil, types.ErrComponentNotFound
		}
		return s.ShardStore, nil

	default:
		return nil, types.ErrComponentNotFound
	}
}

func (s *Service) Start(ch *types.StorageEventChan) {
	if s.ShardStore != nil {
		s.ShardStore.Start(ch)
	}
}

func (s *Service) Stop() {
	if s.ShardStore != nil {
		s.ShardStore.Stop()
	}
}
