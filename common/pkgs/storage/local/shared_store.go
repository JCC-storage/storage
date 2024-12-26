package local

import (
	"io"
	"os"
	"path/filepath"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type SharedStore struct {
	svc *Service
	cfg cdssdk.LocalSharedStorage
}

func NewSharedStore(svc *Service, cfg cdssdk.LocalSharedStorage) (*SharedStore, error) {
	return &SharedStore{
		svc: svc,
		cfg: cfg,
	}, nil
}

func (s *SharedStore) Start(ch *types.StorageEventChan) {
	s.getLogger().Infof("component start, LoadBase: %v", s.cfg.LoadBase)
}

func (s *SharedStore) Stop() {
	s.getLogger().Infof("component stop")
}

func (s *SharedStore) Write(objPath string, stream io.Reader) error {
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

func (s *SharedStore) getLogger() logger.Logger {
	return logger.WithField("SharedStore", "Local").WithField("Storage", s.svc.Detail.Storage.String())
}
