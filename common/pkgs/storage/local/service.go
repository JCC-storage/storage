package local

import (
	"reflect"

	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type Service struct {
	Detail      stgmod.StorageDetail
	ShardStore  *ShardStore
	SharedStore *SharedStore
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

	case reflect2.TypeOf[types.SharedStore]():
		if s.SharedStore == nil {
			return nil, types.ErrComponentNotFound
		}
		return s.SharedStore, nil

	default:
		return nil, types.ErrComponentNotFound
	}
}

func (s *Service) Start(ch *types.StorageEventChan) {
	if s.ShardStore != nil {
		s.ShardStore.Start(ch)
	}

	if s.SharedStore != nil {
		s.SharedStore.Start(ch)
	}
}

func (s *Service) Stop() {
	if s.ShardStore != nil {
		s.ShardStore.Stop()
	}

	if s.SharedStore != nil {
		s.SharedStore.Stop()
	}
}
