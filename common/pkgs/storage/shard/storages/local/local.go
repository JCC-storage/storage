package local

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/storages/utils"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/types"
	stypes "gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

const (
	TempDir   = "tmp"
	BlocksDir = "blocks"
)

type Local struct {
	cfg cdssdk.LocalShardStorage
}

func New(stg cdssdk.Storage, cfg cdssdk.LocalShardStorage) (*Local, error) {
	_, ok := stg.Address.(*cdssdk.LocalStorageAddress)
	if !ok {
		return nil, fmt.Errorf("storage address(%T) is not local", stg)
	}

	return &Local{
		cfg: cfg,
	}, nil
}

func (s *Local) Start(ch *stypes.StorageEventChan) {

}

func (s *Local) Stop() {
}

func (s *Local) New() types.Writer {
	file, err := os.CreateTemp(filepath.Join(s.cfg.Root, "tmp"), "tmp-*")
	if err != nil {
		return utils.ErrorWriter(err)
	}

	return &Writer{
		path:   filepath.Join(s.cfg.Root, "tmp", file.Name()),
		file:   file,
		hasher: sha256.New(),
		owner:  s,
	}
}

// 使用F函数创建Option对象
func (s *Local) Open(opt types.OpenOption) (io.ReadCloser, error) {
	fileName := string(opt.FileHash)
	if len(fileName) < 2 {
		return nil, fmt.Errorf("invalid file name")
	}

	filePath := filepath.Join(s.cfg.Root, BlocksDir, fileName)
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

func (s *Local) ListAll() ([]types.FileInfo, error) {
	var infos []types.FileInfo

	blockDir := filepath.Join(s.cfg.Root, BlocksDir)
	err := filepath.WalkDir(blockDir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		info, ok := d.(fs.FileInfo)
		if !ok {
			return nil
		}

		// TODO 简单检查一下文件名是否合法

		infos = append(infos, types.FileInfo{
			Hash:        cdssdk.FileHash(info.Name()),
			Size:        info.Size(),
			Description: filepath.Join(blockDir, path),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return infos, nil
}

func (s *Local) Purge(removes []cdssdk.FileHash) error {
	for _, hash := range removes {
		fileName := string(hash)

		path := filepath.Join(s.cfg.Root, BlocksDir, fileName[:2], fileName)
		err := os.Remove(path)
		if err != nil {
			logger.Warnf("remove file %v: %v", path, err)
		}
	}

	// TODO 无法保证原子性，所以删除失败只打日志
	return nil
}

func (s *Local) Stats() types.Stats {
	// TODO 统计本地存储的相关信息
	return types.Stats{
		Status: types.StatusOK,
	}
}

func (s *Local) onWritterAbort(w *Writer) {
	logger.Debugf("writting file %v aborted", w.path)
	s.removeTempFile(w.path)
}

func (s *Local) onWritterFinish(w *Writer, hash cdssdk.FileHash) (types.FileInfo, error) {
	logger.Debugf("write file %v finished, size: %v, hash: %v", w.path, w.size, hash)

	blockDir := filepath.Join(s.cfg.Root, BlocksDir, string(hash)[:2])
	err := os.MkdirAll(blockDir, 0755)
	if err != nil {
		s.removeTempFile(w.path)
		logger.Warnf("make block dir %v: %v", blockDir, err)
		return types.FileInfo{}, fmt.Errorf("making block dir: %w", err)
	}

	name := filepath.Join(blockDir, string(hash))
	err = os.Rename(w.path, name)
	if err != nil {
		s.removeTempFile(w.path)
		logger.Warnf("rename %v to %v: %v", w.path, name, err)
		return types.FileInfo{}, fmt.Errorf("rename file: %w", err)
	}

	return types.FileInfo{
		Hash:        hash,
		Size:        w.size,
		Description: w.path,
	}, nil
}

func (s *Local) removeTempFile(path string) {
	err := os.Remove(path)
	if err != nil {
		logger.Warnf("removing temp file %v: %v", path, err)
	}
}
