package metacache

import "time"

type MetaCache interface {
	ClearOutdated()
}

type MetaCacheHost struct {
	caches []MetaCache
}

func NewHost() *MetaCacheHost {
	return &MetaCacheHost{}
}

func (m *MetaCacheHost) Serve() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			for _, cache := range m.caches {
				cache.ClearOutdated()
			}
		}
	}
}
