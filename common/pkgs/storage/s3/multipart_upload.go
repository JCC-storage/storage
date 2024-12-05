package s3

import (
	"context"
	"io"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/os2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type MultipartInitiator struct {
	cli          *s3.Client
	bucket       string
	tempDir      string
	tempFileName string
	tempFilePath string
	uploadID     string
}

func (i *MultipartInitiator) Initiate(ctx context.Context) (types.MultipartInitState, error) {
	i.tempFileName = os2.GenerateRandomFileName(10)
	i.tempFilePath = filepath.Join(i.tempDir, i.tempFileName)

	resp, err := i.cli.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(i.bucket),
		Key:               aws.String(i.tempFilePath),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return types.MultipartInitState{}, err
	}

	i.uploadID = *resp.UploadId

	return types.MultipartInitState{
		UploadID: *resp.UploadId,
		Bucket:   i.bucket,
		Key:      i.tempFilePath,
	}, nil
}

func (i *MultipartInitiator) JoinParts(ctx context.Context, parts []types.UploadedPartInfo) (types.BypassFileInfo, error) {
	s3Parts := make([]s3types.CompletedPart, len(parts))
	for i, part := range parts {
		s3Parts[i] = s3types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(int32(part.PartNumber)),
		}
	}

	compResp, err := i.cli.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(i.bucket),
		Key:      aws.String(i.tempFilePath),
		UploadId: aws.String(i.uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: s3Parts,
		},
	})
	if err != nil {
		return types.BypassFileInfo{}, err
	}

	headResp, err := i.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(i.bucket),
		Key:    aws.String(i.tempFilePath),
	})
	if err != nil {
		return types.BypassFileInfo{}, err
	}

	var hash cdssdk.FileHash
	// if compResp.ChecksumSHA256 == nil {
	// 	hash = "4D142C458F2399175232D5636235B09A84664D60869E925EB20FFBE931045BDD"
	// } else {
	// }
	// TODO2 这里其实是单独上传的每一个分片的SHA256按顺序组成一个新字符串后，再计算得到的SHA256，不是完整文件的SHA256。
	// 这种Hash考虑使用特殊的格式来区分
	hash, err = DecodeBase64Hash(*compResp.ChecksumSHA256)
	if err != nil {
		return types.BypassFileInfo{}, err
	}

	return types.BypassFileInfo{
		TempFilePath: i.tempFilePath,
		Size:         *headResp.ContentLength,
		FileHash:     hash,
	}, nil

}

func (i *MultipartInitiator) Complete() {

}

func (i *MultipartInitiator) Abort() {
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

type MultipartUploader struct {
	cli    *s3.Client
	bucket string
}

func (u *MultipartUploader) UploadPart(ctx context.Context, init types.MultipartInitState, partSize int64, partNumber int, stream io.Reader) (types.UploadedPartInfo, error) {
	resp, err := u.cli.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(init.Bucket),
		Key:        aws.String(init.Key),
		UploadId:   aws.String(init.UploadID),
		PartNumber: aws.Int32(int32(partNumber)),
		Body:       stream,
	})
	if err != nil {
		return types.UploadedPartInfo{}, err
	}

	return types.UploadedPartInfo{
		ETag:       *resp.ETag,
		PartNumber: partNumber,
	}, nil
}

func (u *MultipartUploader) Close() {

}
