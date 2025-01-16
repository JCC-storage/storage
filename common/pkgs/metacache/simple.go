package metacache

import (
	"sync"
	"time"
)

type SimpleMetaCacheConfig[K comparable, V any] struct {
	Getter Getter[K, V]
	Expire time.Duration
}

type Getter[K comparable, V any] func(keys []K) ([]V, []bool)

type SimpleMetaCache[K comparable, V any] struct {
	lock  sync.RWMutex
	cache map[K]*CacheEntry[K, V]
	cfg   SimpleMetaCacheConfig[K, V]
}

func NewSimpleMetaCache[K comparable, V any](cfg SimpleMetaCacheConfig[K, V]) *SimpleMetaCache[K, V] {
	return &SimpleMetaCache[K, V]{
		cache: make(map[K]*CacheEntry[K, V]),
		cfg:   cfg,
	}
}

func (mc *SimpleMetaCache[K, V]) Get(key K) (V, bool) {
	var ret V
	var ok bool

	for i := 0; i < 2; i++ {
		mc.lock.RLock()
		entry, o := mc.cache[key]
		if o {
			ret = entry.Data
			ok = true
		}
		mc.lock.RUnlock()

		if o {
			break
		}

		mc.load([]K{key})
	}

	return ret, ok
}

func (mc *SimpleMetaCache[K, V]) GetMany(keys []K) ([]V, []bool) {
	result := make([]V, len(keys))
	oks := make([]bool, len(keys))

	for i := 0; i < 2; i++ {
		allGet := true
		mc.lock.RLock()
		for i, key := range keys {
			entry, ok := mc.cache[key]
			if ok {
				result[i] = entry.Data
				oks[i] = true
			} else {
				allGet = false
			}
		}
		mc.lock.RUnlock()

		if allGet {
			break
		}

		mc.load(keys)
	}

	return result, oks
}

func (mc *SimpleMetaCache[K, V]) load(keys []K) {
	vs, getOks := mc.cfg.Getter(keys)

	mc.lock.Lock()
	defer mc.lock.Unlock()

	for i, key := range keys {
		if !getOks[i] {
			continue
		}

		_, ok := mc.cache[key]
		// 缓存中已有key则认为缓存中是最新的，不再更新
		if ok {
			continue
		}

		entry := &CacheEntry[K, V]{
			Key:        key,
			Data:       vs[i],
			UpdateTime: time.Now(),
		}
		mc.cache[key] = entry
	}
}

func (mc *SimpleMetaCache[K, V]) ClearOutdated() {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	for key, entry := range mc.cache {
		dt := time.Since(entry.UpdateTime)
		if dt > mc.cfg.Expire || dt < 0 {
			delete(mc.cache, key)
		}
	}
}

type CacheEntry[K comparable, V any] struct {
	Key        K
	Data       V
	UpdateTime time.Time
}
