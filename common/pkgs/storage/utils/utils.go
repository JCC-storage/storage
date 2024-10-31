package utils

import "gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"

type errorShardWriter struct {
	err error
}

func (w *errorShardWriter) Write(data []byte) (int, error) {
	return 0, w.err
}

// 取消写入。要求允许在调用了Finish之后再调用此函数，且此时不应该有任何影响。
// 方便defer机制
func (w *errorShardWriter) Abort() error {
	return w.err
}

// 结束写入，获得文件哈希值
func (w *errorShardWriter) Finish() (types.FileInfo, error) {
	return types.FileInfo{}, w.err
}

func ErrorShardWriter(err error) types.ShardWriter {
	return &errorShardWriter{err: err}
}
