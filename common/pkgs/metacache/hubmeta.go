package metacache

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (m *MetaCacheHost) AddHubMeta() *HubMeta {
	meta := &HubMeta{}
	meta.cache = NewSimpleMetaCache(SimpleMetaCacheConfig[cdssdk.HubID, cdssdk.Hub]{
		Getter: meta.load,
		Expire: time.Minute * 5,
	})

	m.caches = append(m.caches, meta)
	return meta
}

type HubMeta struct {
	cache *SimpleMetaCache[cdssdk.HubID, cdssdk.Hub]
}

func (h *HubMeta) Get(hubID cdssdk.HubID) *cdssdk.Hub {
	v, ok := h.cache.Get(hubID)
	if ok {
		return &v
	}
	return nil
}

func (h *HubMeta) GetMany(hubIDs []cdssdk.HubID) []*cdssdk.Hub {
	vs, oks := h.cache.GetMany(hubIDs)
	ret := make([]*cdssdk.Hub, len(vs))
	for i := range vs {
		if oks[i] {
			ret[i] = &vs[i]
		}
	}
	return ret
}

func (h *HubMeta) ClearOutdated() {
	h.cache.ClearOutdated()
}

func (h *HubMeta) load(keys []cdssdk.HubID) ([]cdssdk.Hub, []bool) {
	vs := make([]cdssdk.Hub, len(keys))
	oks := make([]bool, len(keys))

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		logger.Warnf("new coordinator client: %v", err)
		return vs, oks
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	get, err := coorCli.GetHubs(coormq.NewGetHubs(keys))
	if err != nil {
		logger.Warnf("get hubs: %v", err)
		return vs, oks
	}

	for i := range keys {
		if get.Hubs[i] != nil {
			vs[i] = *get.Hubs[i]
			oks[i] = true
		}
	}

	return vs, oks
}
