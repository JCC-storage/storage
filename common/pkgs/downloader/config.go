package downloader

type Config struct {
	// EC模式的Object的条带缓存数量
	MaxStripCacheCount int `json:"maxStripCacheCount"`
	// EC模式下，每个Object的条带的预取数量，最少为1
	ECStripPrefetchCount int `json:"ecStripPrefetchCount"`
}
