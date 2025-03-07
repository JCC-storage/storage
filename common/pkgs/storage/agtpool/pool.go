package agtpool

import (
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/async"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type storage struct {
	Agent types.StorageAgent
}

type AgentPool struct {
	storages  map[cdssdk.StorageID]*storage
	lock      sync.Mutex
	eventChan *types.StorageEventChan
}

func NewPool() *AgentPool {
	return &AgentPool{
		storages:  make(map[cdssdk.StorageID]*storage),
		eventChan: async.NewUnboundChannel[types.StorageEvent](),
	}
}

func (m *AgentPool) SetupAgent(detail stgmod.StorageDetail) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.storages[detail.Storage.StorageID]; ok {
		return types.ErrStorageExists
	}

	stg := &storage{}

	bld := factory.GetBuilder(detail)
	svc, err := bld.CreateAgent()
	if err != nil {
		return err
	}

	stg.Agent = svc
	m.storages[detail.Storage.StorageID] = stg

	svc.Start(m.eventChan)
	return nil
}

func (m *AgentPool) GetInfo(stgID cdssdk.StorageID) (stgmod.StorageDetail, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stg := m.storages[stgID]
	if stg == nil {
		return stgmod.StorageDetail{}, types.ErrStorageNotFound
	}

	return stg.Agent.Info(), nil
}

func (m *AgentPool) GetAgent(stgID cdssdk.StorageID) (types.StorageAgent, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stg := m.storages[stgID]
	if stg == nil {
		return nil, types.ErrStorageNotFound
	}

	return stg.Agent, nil
}

func (m *AgentPool) GetAllAgents() []types.StorageAgent {
	m.lock.Lock()
	defer m.lock.Unlock()

	agents := make([]types.StorageAgent, 0, len(m.storages))
	for _, stg := range m.storages {
		agents = append(agents, stg.Agent)
	}

	return agents
}

// 查找指定Storage的ShardStore组件
func (m *AgentPool) GetShardStore(stgID cdssdk.StorageID) (types.ShardStore, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stg := m.storages[stgID]
	if stg == nil {
		return nil, types.ErrStorageNotFound
	}

	return stg.Agent.GetShardStore()
}

// 查找指定Storage的PublicStore组件
func (m *AgentPool) GetPublicStore(stgID cdssdk.StorageID) (types.PublicStore, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stg := m.storages[stgID]
	if stg == nil {
		return nil, types.ErrStorageNotFound
	}

	return stg.Agent.GetPublicStore()
}
