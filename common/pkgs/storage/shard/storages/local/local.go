package local

import (
	"io"

	"gitlink.org.cn/cloudream/storage/common/pkgs/shardstore/types"
)

type Local struct {
	cfg Config
}

func (s *Local) New() io.Writer {

}

// 使用F函数创建Option对象
func (s *Local) Open(opt types.OpenOption) (io.ReadCloser, error) {

}

func (s *Local) Remove(hash types.FileHash) error {

}

// 遍历所有文件，callback返回false则停止遍历
func (s *Local) Walk(callback func(info types.FileInfo) bool) error {

}

func (s *Local) Stats() (types.Stats, error) {

}
