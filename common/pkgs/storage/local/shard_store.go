package local

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

const (
	TempDir   = "tmp"
	BlocksDir = "blocks"
)

type ShardStore struct {
	svc              *Service
	cfg              cdssdk.LocalShardStorage
	absRoot          string
	lock             sync.Mutex
	workingTempFiles map[string]bool
	done             chan any
}

func NewShardStore(svc *Service, cfg cdssdk.LocalShardStorage) (*ShardStore, error) {
	absRoot, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("get abs root: %w", err)
	}

	return &ShardStore{
		svc:              svc,
		cfg:              cfg,
		absRoot:          absRoot,
		workingTempFiles: make(map[string]bool),
		done:             make(chan any, 1),
	}, nil
}

func (s *ShardStore) Start(ch *types.StorageEventChan) {
	s.getLogger().Infof("component start, root: %v, max size: %v", s.absRoot, s.cfg.MaxSize)

	go func() {
		removeTempTicker := time.NewTicker(time.Minute * 10)
		defer removeTempTicker.Stop()

		for {
			select {
			case <-removeTempTicker.C:
				s.removeUnusedTempFiles()
			case <-s.done:
				return
			}
		}
	}()
}

func (s *ShardStore) removeUnusedTempFiles() {
	s.lock.Lock()
	defer s.lock.Unlock()

	log := s.getLogger()

	entries, err := os.ReadDir(filepath.Join(s.absRoot, TempDir))
	if err != nil {
		log.Warnf("read temp dir: %v", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if s.workingTempFiles[entry.Name()] {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			log.Warnf("get temp file %v info: %v", entry.Name(), err)
			continue
		}

		path := filepath.Join(s.absRoot, TempDir, entry.Name())
		err = os.Remove(path)
		if err != nil {
			log.Warnf("remove temp file %v: %v", path, err)
		} else {
			log.Infof("remove unused temp file %v, size: %v, last mod time: %v", path, info.Size(), info.ModTime())
		}
	}
}

func (s *ShardStore) Stop() {
	s.getLogger().Infof("component stop")

	select {
	case s.done <- nil:
	default:
	}
}

func (s *ShardStore) Create(stream io.Reader) (types.FileInfo, error) {
	file, err := s.createTempFile()
	if err != nil {
		return types.FileInfo{}, err
	}

	size, hash, err := s.writeTempFile(file, stream)
	if err != nil {
		// Name是文件完整路径
		s.onCreateFailed(file.Name())
		return types.FileInfo{}, err
	}

	return s.onCreateFinished(file.Name(), size, hash)
}

func (s *ShardStore) createTempFile() (*os.File, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	tmpDir := filepath.Join(s.absRoot, TempDir)

	err := os.MkdirAll(tmpDir, 0755)
	if err != nil {
		s.lock.Unlock()
		return nil, err
	}

	file, err := os.CreateTemp(tmpDir, "tmp-*")
	if err != nil {
		s.lock.Unlock()
		return nil, err
	}

	s.workingTempFiles[filepath.Base(file.Name())] = true

	return file, nil
}

func (s *ShardStore) writeTempFile(file *os.File, stream io.Reader) (int64, cdssdk.FileHash, error) {
	defer file.Close()

	buf := make([]byte, 32*1024)
	size := int64(0)

	hasher := sha256.New()
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			size += int64(n)
			io2.WriteAll(hasher, buf[:n])
			err := io2.WriteAll(file, buf[:n])
			if err != nil {
				return 0, "", err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, "", err
		}
	}

	h := hasher.Sum(nil)
	return size, cdssdk.FileHash(strings.ToUpper(hex.EncodeToString(h))), nil
}

func (s *ShardStore) onCreateFinished(tempFilePath string, size int64, hash cdssdk.FileHash) (types.FileInfo, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	defer delete(s.workingTempFiles, filepath.Base(tempFilePath))

	log := s.getLogger()

	log.Debugf("write file %v finished, size: %v, hash: %v", tempFilePath, size, hash)

	blockDir := s.getFileDirFromHash(hash)
	err := os.MkdirAll(blockDir, 0755)
	if err != nil {
		s.removeTempFile(tempFilePath)
		log.Warnf("make block dir %v: %v", blockDir, err)
		return types.FileInfo{}, fmt.Errorf("making block dir: %w", err)
	}

	newPath := filepath.Join(blockDir, string(hash))
	_, err = os.Stat(newPath)
	if os.IsNotExist(err) {
		err = os.Rename(tempFilePath, newPath)
		if err != nil {
			s.removeTempFile(tempFilePath)
			log.Warnf("rename %v to %v: %v", tempFilePath, newPath, err)
			return types.FileInfo{}, fmt.Errorf("rename file: %w", err)
		}

	} else if err != nil {
		s.removeTempFile(tempFilePath)
		log.Warnf("get file %v stat: %v", newPath, err)
		return types.FileInfo{}, fmt.Errorf("get file stat: %w", err)
	} else {
		s.removeTempFile(tempFilePath)
	}

	return types.FileInfo{
		Hash:        hash,
		Size:        size,
		Description: tempFilePath,
	}, nil
}

func (s *ShardStore) onCreateFailed(tempFilePath string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.getLogger().Debugf("writting file %v aborted", tempFilePath)
	s.removeTempFile(tempFilePath)
	delete(s.workingTempFiles, filepath.Base(tempFilePath))
}

func (s *ShardStore) removeTempFile(path string) {
	err := os.Remove(path)
	if err != nil {
		s.getLogger().Warnf("removing temp file %v: %v", path, err)
	}
}

// 使用NewOpen函数创建Option对象
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

	blockDir := filepath.Join(s.absRoot, BlocksDir)
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

func (s *ShardStore) GC(avaiables []cdssdk.FileHash) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	avais := make(map[cdssdk.FileHash]bool)
	for _, hash := range avaiables {
		avais[hash] = true
	}

	cnt := 0

	blockDir := filepath.Join(s.absRoot, BlocksDir)
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

		fileHash := cdssdk.FileHash(filepath.Base(info.Name()))
		if !avais[fileHash] {
			err = os.Remove(path)
			if err != nil {
				s.getLogger().Warnf("remove file %v: %v", path, err)
			} else {
				cnt++
			}
		}

		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
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

func (s *ShardStore) BypassUploaded(info types.BypassFileInfo) error {
	if info.FileHash == "" {
		return fmt.Errorf("empty file hash is not allowed by this shard store")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	log := s.getLogger()

	log.Debugf("%v bypass uploaded, size: %v, hash: %v", info.TempFilePath, info.Size, info.FileHash)

	blockDir := s.getFileDirFromHash(info.FileHash)
	err := os.MkdirAll(blockDir, 0755)
	if err != nil {
		log.Warnf("make block dir %v: %v", blockDir, err)
		return fmt.Errorf("making block dir: %w", err)
	}

	newPath := filepath.Join(blockDir, string(info.FileHash))
	_, err = os.Stat(newPath)
	if os.IsNotExist(err) {
		err = os.Rename(info.TempFilePath, newPath)
		if err != nil {
			log.Warnf("rename %v to %v: %v", info.TempFilePath, newPath, err)
			return fmt.Errorf("rename file: %w", err)
		}

	} else if err != nil {
		log.Warnf("get file %v stat: %v", newPath, err)
		return fmt.Errorf("get file stat: %w", err)
	}

	return nil
}

func (s *ShardStore) getLogger() logger.Logger {
	return logger.WithField("ShardStore", "Local").WithField("Storage", s.svc.Detail.Storage.String())
}

func (s *ShardStore) getFileDirFromHash(hash cdssdk.FileHash) string {
	return filepath.Join(s.absRoot, BlocksDir, string(hash)[:2])
}

func (s *ShardStore) getFilePathFromHash(hash cdssdk.FileHash) string {
	return filepath.Join(s.absRoot, BlocksDir, string(hash)[:2], string(hash))
}
