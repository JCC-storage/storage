package mgr

import (
	"errors"
	"reflect"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/async"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

var ErrStorageNotFound = errors.New("storage not found")

var ErrComponentNotFound = errors.New("component not found")

var ErrStorageExists = errors.New("storage already exists")

type storage struct {
	Shard      types.ShardStore
	Shared     types.SharedStore
	Temp       types.TempStore
	Components []types.StorageComponent
}

type Manager struct {
	storages  map[cdssdk.StorageID]*storage
	lock      sync.Mutex
	eventChan *types.StorageEventChan
}

func NewManager() *Manager {
	return &Manager{
		storages:  make(map[cdssdk.StorageID]*storage),
		eventChan: async.NewUnboundChannel[types.StorageEvent](),
	}
}

func (m *Manager) InitStorage(detail stgmod.StorageDetail) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.storages[detail.Storage.StorageID]; ok {
		return ErrStorageExists
	}

	stg := &storage{}

	if detail.Shard != nil {
		err := createShardStore(detail, m.eventChan, stg)
		if err != nil {
			stopStorage(stg)
			return err
		}
	}

	if detail.Shared != nil {
		err := createSharedStore(detail, m.eventChan, stg)
		if err != nil {
			stopStorage(stg)
			return err
		}
	}

	// 创建其他组件
	err := createComponents(detail, m.eventChan, stg)
	if err != nil {
		stopStorage(stg)
		return err
	}

	m.storages[detail.Storage.StorageID] = stg
	return nil
}

func stopStorage(stg *storage) {
	if stg.Shard != nil {
		stg.Shard.Stop()
	}

	if stg.Shared != nil {
		stg.Shared.Stop()
	}

	for _, c := range stg.Components {
		c.Stop()
	}
}

// 查找指定Storage的ShardStore组件
func (m *Manager) GetShardStore(stgID cdssdk.StorageID) (types.ShardStore, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stg := m.storages[stgID]
	if stg == nil {
		return nil, ErrStorageNotFound
	}

	if stg.Shard == nil {
		return nil, ErrComponentNotFound
	}

	return stg.Shard, nil
}

// 查找指定Storage的SharedStore组件
func (m *Manager) GetSharedStore(stgID cdssdk.StorageID) (types.SharedStore, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stg := m.storages[stgID]
	if stg == nil {
		return nil, ErrStorageNotFound
	}

	if stg.Shared == nil {
		return nil, ErrComponentNotFound
	}

	return stg.Shared, nil
}

func (m *Manager) GetTempStore(stgID cdssdk.StorageID) (types.TempStore, error) {
	return nil, nil
}

// 查找指定Storage的指定类型的组件，可以是ShardStore、SharedStore、或者其他自定义的组件
func (m *Manager) GetComponent(stgID cdssdk.StorageID, typ reflect.Type) (types.StorageComponent, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stg := m.storages[stgID]
	if stg == nil {
		return nil, ErrStorageNotFound
	}

	switch typ {
	case reflect2.TypeOf[types.ShardStore]():
		if stg.Shard == nil {
			return nil, ErrComponentNotFound
		}

		return stg.Shard, nil
	case reflect2.TypeOf[types.SharedStore]():
		if stg.Shared == nil {
			return nil, ErrComponentNotFound
		}

		return stg.Shared, nil
	default:
		for _, c := range stg.Components {
			if reflect.TypeOf(c) == typ {
				return c, nil
			}
		}

		return nil, ErrComponentNotFound
	}
}

func GetComponent[T types.StorageComponent](mgr *Manager, stgID cdssdk.StorageID) (T, error) {
	ret, err := mgr.GetComponent(stgID, reflect2.TypeOf[T]())
	if err != nil {
		var def T
		return def, err
	}

	return ret.(T), nil
}
