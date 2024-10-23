package local

import "gitlink.org.cn/cloudream/storage/common/pkgs/shardstore/types"

type Config struct {
	RootPath string
	MaxSize  int64
}

func (c *Config) Build() types.ShardStore {
	return &Local{cfg: *c}
}
