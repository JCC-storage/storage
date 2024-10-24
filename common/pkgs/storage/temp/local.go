package tempstore

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type LocalTempStore struct {
	cfg cdssdk.BypassUploadFeature
}

func NewLocalTempStore(cfg cdssdk.BypassUploadFeature) *LocalTempStore {
	return &LocalTempStore{
		cfg: cfg,
	}
}
