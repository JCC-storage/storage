package s3

import (
	"context"
	"crypto/sha256"
	"io"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/os2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type Multiparter struct {
	detail stgmod.StorageDetail
	feat   *cdssdk.MultipartUploadFeature
	bucket string
	cli    *s3.Client
}

func NewMultiparter(detail stgmod.StorageDetail, feat *cdssdk.MultipartUploadFeature, bkt string, cli *s3.Client) *Multiparter {
	return &Multiparter{
		detail: detail,
		feat:   feat,
		bucket: bkt,
		cli:    cli,
	}
}

func (m *Multiparter) MinPartSize() int64 {
	return m.feat.MinPartSize
}

func (m *Multiparter) MaxPartSize() int64 {
	return m.feat.MaxPartSize
}

func (m *Multiparter) Initiate(ctx context.Context) (types.MultipartTask, error) {
	tempFileName := os2.GenerateRandomFileName(10)
	tempFilePath := filepath.Join(m.feat.TempDir, tempFileName)

	resp, err := m.cli.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(m.bucket),
		Key:               aws.String(tempFilePath),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return nil, err
	}

	return &MultipartTask{
		cli:          m.cli,
		bucket:       m.bucket,
		tempDir:      m.feat.TempDir,
		tempFileName: tempFileName,
		tempFilePath: tempFilePath,
		uploadID:     *resp.UploadId,
	}, nil
}

func (m *Multiparter) UploadPart(ctx context.Context, init types.MultipartInitState, partSize int64, partNumber int, stream io.Reader) (types.UploadedPartInfo, error) {
	hashStr := io2.NewReadHasher(sha256.New(), stream)
	resp, err := m.cli.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(init.Bucket),
		Key:        aws.String(init.Key),
		UploadId:   aws.String(init.UploadID),
		PartNumber: aws.Int32(int32(partNumber)),
		Body:       hashStr,
	})
	if err != nil {
		return types.UploadedPartInfo{}, err
	}

	return types.UploadedPartInfo{
		ETag:       *resp.ETag,
		PartNumber: partNumber,
		PartHash:   hashStr.Sum(),
	}, nil
}

type MultipartTask struct {
	cli          *s3.Client
	bucket       string
	tempDir      string
	tempFileName string
	tempFilePath string
	uploadID     string
}

func (i *MultipartTask) InitState() types.MultipartInitState {
	return types.MultipartInitState{
		UploadID: i.uploadID,
		Bucket:   i.bucket,
		Key:      i.tempFilePath,
	}
}

func (i *MultipartTask) JoinParts(ctx context.Context, parts []types.UploadedPartInfo) (types.BypassUploadedFile, error) {
	parts = sort2.Sort(parts, func(l, r types.UploadedPartInfo) int {
		return l.PartNumber - r.PartNumber
	})

	s3Parts := make([]s3types.CompletedPart, len(parts))
	for i, part := range parts {
		s3Parts[i] = s3types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(int32(part.PartNumber)),
		}
	}
	partHashes := make([][]byte, len(parts))
	for i, part := range parts {
		partHashes[i] = part.PartHash
	}

	_, err := i.cli.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(i.bucket),
		Key:      aws.String(i.tempFilePath),
		UploadId: aws.String(i.uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: s3Parts,
		},
	})
	if err != nil {
		return types.BypassUploadedFile{}, err
	}

	headResp, err := i.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(i.bucket),
		Key:    aws.String(i.tempFilePath),
	})
	if err != nil {
		return types.BypassUploadedFile{}, err
	}

	hash := cdssdk.CalculateCompositeHash(partHashes)

	return types.BypassUploadedFile{
		Path: i.tempFilePath,
		Size: *headResp.ContentLength,
		Hash: hash,
	}, nil

}

func (i *MultipartTask) Complete() {

}

func (i *MultipartTask) Abort() {
	// TODO2 根据注释描述，Abort不能停止正在上传的分片，需要等待其上传完成才能彻底删除，
	// 考虑增加定时任务去定时清理
	i.cli.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(i.bucket),
		Key:      aws.String(i.tempFilePath),
		UploadId: aws.String(i.uploadID),
	})
	i.cli.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(i.bucket),
		Key:    aws.String(i.tempFilePath),
	})
}
