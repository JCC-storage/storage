package oss

import (
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"io"
	"log"
)

type MultiPartUploader struct {
	client *oss.Client
	bucket *oss.Bucket
}

func NewMultiPartUpload(address *cdssdk.OSSAddress) *MultiPartUploader {
	// 创建OSSClient实例。
	client, err := oss.New(address.Endpoint, address.AK, address.SK)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	bucket, err := client.Bucket(address.Bucket)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	return &MultiPartUploader{
		client: client,
		bucket: bucket,
	}
}

func (c *MultiPartUploader) InitiateMultipartUpload(objectName string) (string, error) {
	imur, err := c.bucket.InitiateMultipartUpload(objectName)
	if err != nil {
		return "", fmt.Errorf("failed to initiate multipart upload: %w", err)
	}
	return imur.UploadID, nil
}

func (c *MultiPartUploader) UploadPart(uploadID string, key string, partSize int64, partNumber int, stream io.Reader) (*types.UploadPartOutput, error) {
	uploadParam := oss.InitiateMultipartUploadResult{
		UploadID: uploadID,
		Key:      key,
		Bucket:   c.bucket.BucketName,
	}
	part, err := c.bucket.UploadPart(uploadParam, stream, partSize, partNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to upload part: %w", err)
	}
	result := &types.UploadPartOutput{
		ETag:       part.ETag,
		PartNumber: partNumber,
	}
	return result, nil
}

func (c *MultiPartUploader) CompleteMultipartUpload(uploadID string, Key string, parts []*types.UploadPartOutput) error {
	notifyParam := oss.InitiateMultipartUploadResult{
		UploadID: uploadID,
		Key:      Key,
		Bucket:   c.bucket.BucketName,
	}
	var uploadPart []oss.UploadPart
	for i := 0; i < len(parts); i++ {
		uploadPart = append(uploadPart, oss.UploadPart{
			PartNumber: parts[i].PartNumber,
			ETag:       parts[i].ETag,
		})
	}
	_, err := c.bucket.CompleteMultipartUpload(notifyParam, uploadPart)
	if err != nil {
		return err
	}
	return nil
}

func (c *MultiPartUploader) AbortMultipartUpload() {

}

func (c *MultiPartUploader) Close() {
	// 关闭client

}
