package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/os2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type MultipartInitiator struct {
	absTempDir     string // 应该要是绝对路径
	tempFileName   string
	tempPartsDir   string
	joinedFilePath string
}

func (i *MultipartInitiator) Initiate(ctx context.Context) (types.MultipartInitState, error) {
	i.tempFileName = os2.GenerateRandomFileName(10)
	i.tempPartsDir = filepath.Join(i.absTempDir, i.tempFileName)
	i.joinedFilePath = filepath.Join(i.absTempDir, i.tempFileName+".joined")

	err := os.MkdirAll(i.tempPartsDir, 0777)

	if err != nil {
		return types.MultipartInitState{}, err
	}

	return types.MultipartInitState{
		UploadID: i.tempPartsDir,
	}, nil
}

func (i *MultipartInitiator) JoinParts(ctx context.Context, parts []types.UploadedPartInfo) (types.BypassFileInfo, error) {
	parts = sort2.Sort(parts, func(l, r types.UploadedPartInfo) int {
		return l.PartNumber - r.PartNumber
	})

	joined, err := os.Create(i.joinedFilePath)
	if err != nil {
		return types.BypassFileInfo{}, err
	}
	defer joined.Close()

	size := int64(0)
	hasher := sha256.New()

	for _, part := range parts {
		partSize, err := i.writePart(part, joined, hasher)
		if err != nil {
			return types.BypassFileInfo{}, err
		}

		size += partSize
	}

	h := hasher.Sum(nil)
	return types.BypassFileInfo{
		TempFilePath: joined.Name(),
		Size:         size,
		FileHash:     cdssdk.FileHash(strings.ToUpper(hex.EncodeToString(h))),
	}, nil
}

func (i *MultipartInitiator) writePart(partInfo types.UploadedPartInfo, joined *os.File, hasher hash.Hash) (int64, error) {
	part, err := os.Open(partInfo.ETag)
	if err != nil {
		return 0, err
	}
	defer part.Close()

	buf := make([]byte, 32*1024)
	size := int64(0)
	for {
		n, err := part.Read(buf)
		if n > 0 {
			size += int64(n)
			io2.WriteAll(hasher, buf[:n])
			err := io2.WriteAll(joined, buf[:n])
			if err != nil {
				return 0, err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}

	return size, nil
}

func (i *MultipartInitiator) Complete() {
	i.Abort()
}

func (i *MultipartInitiator) Abort() {
	os.Remove(i.joinedFilePath)
	os.RemoveAll(i.tempPartsDir)
}

type MultipartUploader struct{}

func (u *MultipartUploader) UploadPart(ctx context.Context, init types.MultipartInitState, partSize int64, partNumber int, stream io.Reader) (types.UploadedPartInfo, error) {
	partFilePath := filepath.Join(init.UploadID, fmt.Sprintf("%v", partNumber))
	partFile, err := os.Create(partFilePath)
	if err != nil {
		return types.UploadedPartInfo{}, err
	}
	defer partFile.Close()

	_, err = io.Copy(partFile, stream)
	if err != nil {
		return types.UploadedPartInfo{}, err
	}
	return types.UploadedPartInfo{
		ETag:       partFilePath,
		PartNumber: partNumber,
	}, nil
}

func (u *MultipartUploader) Close() {

}
