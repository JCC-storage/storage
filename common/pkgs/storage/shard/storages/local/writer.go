package local

import "gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/types"

type Writer struct {
}

func (w *Writer) Write(data []byte) error {

}

// 取消写入
func (w *Writer) Abort() error {

}

// 结束写入，获得文件哈希值
func (w *Writer) Finish() (types.FileInfo, error) {

}
