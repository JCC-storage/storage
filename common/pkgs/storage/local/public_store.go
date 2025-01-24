package local

import (
	"io"
	"os"
	"path/filepath"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type PublicStoreDesc struct {
	types.EmptyPublicStoreDesc
	builder *builder
}

func (d *PublicStoreDesc) Enabled() bool {
	return d.builder.detail.Storage.PublicStore != nil
}

type PublicStore struct {
	agt *agent
	cfg cdssdk.LocalPublicStorage
}

func NewPublicStore(agt *agent, cfg cdssdk.LocalPublicStorage) (*PublicStore, error) {
	return &PublicStore{
		agt: agt,
		cfg: cfg,
	}, nil
}

func (s *PublicStore) Start(ch *types.StorageEventChan) {
	s.getLogger().Infof("component start, LoadBase: %v", s.cfg.LoadBase)
}

func (s *PublicStore) Stop() {
	s.getLogger().Infof("component stop")
}

func (s *PublicStore) Write(objPath string, stream io.Reader) error {
	fullPath := filepath.Join(s.cfg.LoadBase, objPath)
	err := os.MkdirAll(filepath.Dir(fullPath), 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, stream)
	if err != nil {
		return err
	}

	return nil
}

func (s *PublicStore) getLogger() logger.Logger {
	return logger.WithField("PublicStore", "Local").WithField("Storage", s.agt.Detail.Storage.String())
}
