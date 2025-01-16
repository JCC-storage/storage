package metacache

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (m *MetaCacheHost) AddStorageMeta() *StorageMeta {
	meta := &StorageMeta{}
	meta.cache = NewSimpleMetaCache(SimpleMetaCacheConfig[cdssdk.StorageID, stgmod.StorageDetail]{
		Getter: meta.load,
		Expire: time.Minute * 5,
	})

	m.caches = append(m.caches, meta)
	return meta
}

type StorageMeta struct {
	cache *SimpleMetaCache[cdssdk.StorageID, stgmod.StorageDetail]
}

func (s *StorageMeta) Get(stgID cdssdk.StorageID) *stgmod.StorageDetail {
	v, ok := s.cache.Get(stgID)
	if ok {
		return &v
	}
	return nil
}

func (s *StorageMeta) GetMany(stgIDs []cdssdk.StorageID) []*stgmod.StorageDetail {
	vs, oks := s.cache.GetMany(stgIDs)
	ret := make([]*stgmod.StorageDetail, len(vs))
	for i := range vs {
		if oks[i] {
			ret[i] = &vs[i]
		}
	}
	return ret
}

func (s *StorageMeta) ClearOutdated() {
	s.cache.ClearOutdated()
}

func (s *StorageMeta) load(keys []cdssdk.StorageID) ([]stgmod.StorageDetail, []bool) {
	vs := make([]stgmod.StorageDetail, len(keys))
	oks := make([]bool, len(keys))

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		logger.Warnf("new coordinator client: %v", err)
		return vs, oks
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	get, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails(keys))
	if err != nil {
		logger.Warnf("get storage details: %v", err)
		return vs, oks
	}

	for i := range keys {
		if get.Storages[i] != nil {
			vs[i] = *get.Storages[i]
			oks[i] = true
		}
	}

	return vs, oks
}
