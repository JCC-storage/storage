package svcmgr

import (
	"reflect"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/async"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type storage struct {
	Service types.StorageService
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

func (m *Manager) CreateService(detail stgmod.StorageDetail) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.storages[detail.Storage.StorageID]; ok {
		return types.ErrStorageExists
	}

	stg := &storage{}

	svc, err := factory.CreateService(detail)
	if err != nil {
		return err
	}

	stg.Service = svc
	m.storages[detail.Storage.StorageID] = stg

	svc.Start(m.eventChan)
	return nil
}

// 查找指定Storage的ShardStore组件
func (m *Manager) GetShardStore(stgID cdssdk.StorageID) (types.ShardStore, error) {
	return GetComponent[types.ShardStore](m, stgID)
}

// 查找指定Storage的SharedStore组件
func (m *Manager) GetSharedStore(stgID cdssdk.StorageID) (types.SharedStore, error) {
	return GetComponent[types.SharedStore](m, stgID)
}

// 查找指定Storage的指定类型的组件，可以是ShardStore、SharedStore、或者其他自定义的组件
func (m *Manager) GetComponent(stgID cdssdk.StorageID, typ reflect.Type) (any, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stg := m.storages[stgID]
	if stg == nil {
		return nil, types.ErrStorageNotFound
	}

	return stg.Service.GetComponent(typ)
}

func GetComponent[T any](mgr *Manager, stgID cdssdk.StorageID) (T, error) {
	ret, err := mgr.GetComponent(stgID, reflect2.TypeOf[T]())
	if err != nil {
		var def T
		return def, err
	}

	return ret.(T), nil
}
