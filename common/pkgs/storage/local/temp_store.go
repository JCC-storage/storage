package local

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type LocalTempStore struct {
	cfg cdssdk.BypassWriteFeature
	stg cdssdk.Storage
}

func NewLocalTempStore(stg cdssdk.Storage, cfg cdssdk.BypassWriteFeature) *LocalTempStore {
	return &LocalTempStore{
		cfg: cfg,
		stg: stg,
	}
}
