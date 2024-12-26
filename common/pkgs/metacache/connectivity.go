package metacache

import (
	"sync"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (m *MetaCacheHost) AddConnectivity() *Connectivity {
	cache := &Connectivity{
		entries: make(map[cdssdk.HubID]*ConnectivityEntry),
	}

	m.caches = append(m.caches, cache)
	return cache
}

type Connectivity struct {
	lock    sync.RWMutex
	entries map[cdssdk.HubID]*ConnectivityEntry
}

func (c *Connectivity) Get(from cdssdk.HubID, to cdssdk.HubID) *time.Duration {
	for i := 0; i < 2; i++ {
		c.lock.RLock()
		entry, ok := c.entries[from]
		if ok {
			con, ok := entry.To[to]
			if ok {
				c.lock.RUnlock()

				if con.Latency == nil {
					return nil
				}
				l := time.Millisecond * time.Duration(*con.Latency)
				return &l
			}
		}
		c.lock.RUnlock()

		c.load(from)
	}

	return nil
}

func (c *Connectivity) ClearOutdated() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for hubID, entry := range c.entries {
		if time.Since(entry.UpdateTime) > time.Minute*5 {
			delete(c.entries, hubID)
		}
	}
}

func (c *Connectivity) load(hubID cdssdk.HubID) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		logger.Warnf("new coordinator client: %v", err)
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	get, err := coorCli.GetHubConnectivities(coormq.ReqGetHubConnectivities([]cdssdk.HubID{hubID}))
	if err != nil {
		logger.Warnf("get hub connectivities: %v", err)
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	ce := &ConnectivityEntry{
		From:       hubID,
		To:         make(map[cdssdk.HubID]cdssdk.HubConnectivity),
		UpdateTime: time.Now(),
	}

	for _, conn := range get.Connectivities {
		ce.To[conn.ToHubID] = conn
	}

	c.entries[hubID] = ce
}

type ConnectivityEntry struct {
	From       cdssdk.HubID
	To         map[cdssdk.HubID]cdssdk.HubConnectivity
	UpdateTime time.Time
}
