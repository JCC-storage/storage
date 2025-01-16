package local

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/os2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type Multiparter struct {
	feat *cdssdk.MultipartUploadFeature
}

func (m *Multiparter) MinPartSize() int64 {
	return m.feat.MinPartSize
}

func (m *Multiparter) MaxPartSize() int64 {
	return m.feat.MaxPartSize
}

func (m *Multiparter) Initiate(ctx context.Context) (types.MultipartTask, error) {
	absTempDir, err := filepath.Abs(m.feat.TempDir)
	if err != nil {
		return nil, fmt.Errorf("get abs temp dir %v: %v", m.feat.TempDir, err)
	}

	tempFileName := os2.GenerateRandomFileName(10)
	tempPartsDir := filepath.Join(absTempDir, tempFileName)
	joinedFilePath := filepath.Join(absTempDir, tempFileName+".joined")

	err = os.MkdirAll(tempPartsDir, 0777)

	if err != nil {
		return nil, err
	}

	return &MultipartTask{
		absTempDir:     absTempDir,
		tempFileName:   tempFileName,
		tempPartsDir:   tempPartsDir,
		joinedFilePath: joinedFilePath,
		uploadID:       tempPartsDir,
	}, nil
}

func (m *Multiparter) UploadPart(ctx context.Context, init types.MultipartInitState, partSize int64, partNumber int, stream io.Reader) (types.UploadedPartInfo, error) {
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

type MultipartTask struct {
	absTempDir     string // 应该要是绝对路径
	tempFileName   string
	tempPartsDir   string
	joinedFilePath string
	uploadID       string
}

func (i *MultipartTask) InitState() types.MultipartInitState {
	return types.MultipartInitState{
		UploadID: i.uploadID,
	}
}

func (i *MultipartTask) JoinParts(ctx context.Context, parts []types.UploadedPartInfo) (types.BypassFileInfo, error) {
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
		FileHash:     cdssdk.NewFullHash(h),
	}, nil
}

func (i *MultipartTask) writePart(partInfo types.UploadedPartInfo, joined *os.File, hasher hash.Hash) (int64, error) {
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

func (i *MultipartTask) Complete() {
	i.Abort()
}

func (i *MultipartTask) Abort() {
	os.Remove(i.joinedFilePath)
	os.RemoveAll(i.tempPartsDir)
}
