package utils

import "gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/types"

type errorWriter struct {
	err error
}

func (w *errorWriter) Write(data []byte) (int, error) {
	return 0, w.err
}

// 取消写入。要求允许在调用了Finish之后再调用此函数，且此时不应该有任何影响。
// 方便defer机制
func (w *errorWriter) Abort() error {
	return w.err
}

// 结束写入，获得文件哈希值
func (w *errorWriter) Finish() (types.FileInfo, error) {
	return types.FileInfo{}, w.err
}

func ErrorWriter(err error) types.Writer {
	return &errorWriter{err: err}
}
