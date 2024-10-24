package local

import (
	"io"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/types"
)

type Local struct {
	cfg cdssdk.LocalShardStorage
}

func New(stg cdssdk.Storage, cfg cdssdk.LocalShardStorage) *Local {
	return &Local{
		cfg: cfg,
	}
}

func (s *Local) New() io.Writer {

}

// 使用F函数创建Option对象
func (s *Local) Open(opt types.OpenOption) (io.ReadCloser, error) {

}

func (s *Local) Remove(hash types.FileHash) error {

}

// 遍历所有文件，callback返回false则停止遍历
func (s *Local) ListAll() ([]types.FileInfo, error) {

}

func (s *Local) Purge(availables []types.FileHash) error {

}

func (s *Local) Stats() (types.Stats, error) {

}
