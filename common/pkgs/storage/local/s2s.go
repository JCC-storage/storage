package local

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/os2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type S2STransfer struct {
	feat    cdssdk.S2STransferFeature
	detail  stgmod.StorageDetail
	dstPath string
}

// 只有同一个机器的存储之间才可以进行数据直传
func (s *S2STransfer) CanTransfer(src stgmod.StorageDetail) bool {
	_, ok := src.Storage.Type.(*cdssdk.LocalStorageType)
	if !ok {
		return false
	}

	if src.Storage.MasterHub != s.detail.Storage.MasterHub {
		return false
	}

	return true
}

// 执行数据直传
func (s *S2STransfer) Transfer(ctx context.Context, src stgmod.StorageDetail, srcPath string) (string, error) {
	absTempDir, err := filepath.Abs(s.feat.TempDir)
	if err != nil {
		return "", fmt.Errorf("get abs temp dir %v: %v", s.feat.TempDir, err)
	}

	tempFileName := os2.GenerateRandomFileName(10)
	s.dstPath = filepath.Join(absTempDir, tempFileName)

	copy, err := os.OpenFile(s.dstPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return "", err
	}
	defer copy.Close()

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return "", err
	}
	defer srcFile.Close()

	_, err = io.Copy(copy, srcFile)
	if err != nil {
		return "", err
	}

	return s.dstPath, nil
}

func (s *S2STransfer) Complete() {

}

func (s *S2STransfer) Abort() {
	if s.dstPath != "" {
		os.Remove(s.dstPath)
	}
}
