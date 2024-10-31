package local

import (
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"strings"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type ShardWriter struct {
	path   string
	file   *os.File
	hasher hash.Hash
	size   int64
	closed bool
	owner  *ShardStore
}

func (w *ShardWriter) Write(data []byte) (int, error) {
	n, err := w.file.Write(data)
	if err != nil {
		return 0, err
	}

	w.hasher.Write(data[:n])
	w.size += int64(n)
	return n, nil
}

// 取消写入
func (w *ShardWriter) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true

	err := w.file.Close()
	w.owner.onWritterAbort(w)
	return err
}

// 结束写入，获得文件哈希值
func (w *ShardWriter) Finish() (types.FileInfo, error) {
	if w.closed {
		return types.FileInfo{}, fmt.Errorf("stream closed")
	}
	w.closed = true

	err := w.file.Close()
	if err != nil {
		w.owner.onWritterAbort(w)
		return types.FileInfo{}, err
	}

	sum := w.hasher.Sum(nil)
	info, err := w.owner.onWritterFinish(w, cdssdk.FileHash(strings.ToUpper(hex.EncodeToString(sum))))
	if err != nil {
		// 无需再调onWritterAbort, onWritterFinish会处理
		return types.FileInfo{}, err
	}
	return info, nil
}
