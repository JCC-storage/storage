package local

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

const (
	TempDir   = "tmp"
	BlocksDir = "blocks"
	SvcName   = "LocalShardStore"
)

type ShardStore struct {
	stg  cdssdk.Storage
	cfg  cdssdk.LocalShardStorage
	lock sync.Mutex
}

func NewShardStore(stg cdssdk.Storage, cfg cdssdk.LocalShardStorage) (*ShardStore, error) {
	_, ok := stg.Address.(*cdssdk.LocalStorageAddress)
	if !ok {
		return nil, fmt.Errorf("storage address(%T) is not local", stg)
	}

	return &ShardStore{
		stg: stg,
		cfg: cfg,
	}, nil
}

func (s *ShardStore) Start(ch *types.StorageEventChan) {
	s.getLogger().Infof("local shard store start, root: %v, max size: %v", s.cfg.Root, s.cfg.MaxSize)
}

func (s *ShardStore) Stop() {
	s.getLogger().Infof("local shard store stop")
}

func (s *ShardStore) New() types.ShardWriter {
	s.lock.Lock()
	defer s.lock.Unlock()

	tmpDir := filepath.Join(s.cfg.Root, TempDir)

	err := os.MkdirAll(tmpDir, 0755)
	if err != nil {
		return utils.ErrorShardWriter(err)
	}

	file, err := os.CreateTemp(tmpDir, "tmp-*")
	if err != nil {
		return utils.ErrorShardWriter(err)
	}

	return &ShardWriter{
		path:   file.Name(), // file.Name 包含tmpDir路径
		file:   file,
		hasher: sha256.New(),
		owner:  s,
	}
}

// 使用F函数创建Option对象
func (s *ShardStore) Open(opt types.OpenOption) (io.ReadCloser, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	fileName := string(opt.FileHash)
	if len(fileName) < 2 {
		return nil, fmt.Errorf("invalid file name")
	}

	filePath := s.getFilePathFromHash(cdssdk.FileHash(fileName))
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	if opt.Offset > 0 {
		_, err = file.Seek(opt.Offset, io.SeekStart)
		if err != nil {
			file.Close()
			return nil, err
		}
	}

	if opt.Length >= 0 {
		return io2.Length(file, opt.Length), nil
	}

	return file, nil
}

func (s *ShardStore) Info(hash cdssdk.FileHash) (types.FileInfo, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	filePath := s.getFilePathFromHash(hash)
	info, err := os.Stat(filePath)
	if err != nil {
		return types.FileInfo{}, err
	}

	return types.FileInfo{
		Hash:        hash,
		Size:        info.Size(),
		Description: filePath,
	}, nil
}

func (s *ShardStore) ListAll() ([]types.FileInfo, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var infos []types.FileInfo

	blockDir := filepath.Join(s.cfg.Root, BlocksDir)
	err := filepath.WalkDir(blockDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		// TODO 简单检查一下文件名是否合法

		infos = append(infos, types.FileInfo{
			Hash:        cdssdk.FileHash(filepath.Base(info.Name())),
			Size:        info.Size(),
			Description: filepath.Join(blockDir, path),
		})
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	return infos, nil
}

func (s *ShardStore) Purge(removes []cdssdk.FileHash) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	cnt := 0

	for _, hash := range removes {
		fileName := string(hash)

		path := filepath.Join(s.cfg.Root, BlocksDir, fileName[:2], fileName)
		err := os.Remove(path)
		if err != nil {
			s.getLogger().Warnf("remove file %v: %v", path, err)
		} else {
			cnt++
		}
	}

	s.getLogger().Infof("purge %d files", cnt)

	// TODO 无法保证原子性，所以删除失败只打日志
	return nil
}

func (s *ShardStore) Stats() types.Stats {
	// TODO 统计本地存储的相关信息
	return types.Stats{
		Status: types.StatusOK,
	}
}

func (s *ShardStore) onWritterAbort(w *ShardWriter) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.getLogger().Debugf("writting file %v aborted", w.path)
	s.removeTempFile(w.path)
}

func (s *ShardStore) onWritterFinish(w *ShardWriter, hash cdssdk.FileHash) (types.FileInfo, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	log := s.getLogger()

	log.Debugf("write file %v finished, size: %v, hash: %v", w.path, w.size, hash)

	blockDir := s.getFileDirFromHash(hash)
	err := os.MkdirAll(blockDir, 0755)
	if err != nil {
		s.removeTempFile(w.path)
		log.Warnf("make block dir %v: %v", blockDir, err)
		return types.FileInfo{}, fmt.Errorf("making block dir: %w", err)
	}

	newPath := filepath.Join(blockDir, string(hash))
	_, err = os.Stat(newPath)
	if os.IsNotExist(err) {
		err = os.Rename(w.path, newPath)
		if err != nil {
			s.removeTempFile(w.path)
			log.Warnf("rename %v to %v: %v", w.path, newPath, err)
			return types.FileInfo{}, fmt.Errorf("rename file: %w", err)
		}

	} else if err != nil {
		s.removeTempFile(w.path)
		log.Warnf("get file %v stat: %v", newPath, err)
		return types.FileInfo{}, fmt.Errorf("get file stat: %w", err)
	}

	return types.FileInfo{
		Hash:        hash,
		Size:        w.size,
		Description: w.path,
	}, nil
}

func (s *ShardStore) removeTempFile(path string) {
	err := os.Remove(path)
	if err != nil {
		s.getLogger().Warnf("removing temp file %v: %v", path, err)
	}
}

func (s *ShardStore) getLogger() logger.Logger {
	return logger.WithField("S", SvcName).WithField("Storage", s.stg.String())
}

func (s *ShardStore) getFileDirFromHash(hash cdssdk.FileHash) string {
	return filepath.Join(s.cfg.Root, BlocksDir, string(hash)[:2])
}

func (s *ShardStore) getFilePathFromHash(hash cdssdk.FileHash) string {
	return filepath.Join(s.cfg.Root, BlocksDir, string(hash)[:2], string(hash))
}
