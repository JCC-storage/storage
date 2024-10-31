package oss

import (
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"log"
)

type OSSClient struct {
	client *oss.Client
	bucket *oss.Bucket
}

func (c *OSSClient) InitiateMultipartUpload(objectName string) (string, error) {
	imur, err := c.bucket.InitiateMultipartUpload(objectName)
	if err != nil {
		return "", fmt.Errorf("failed to initiate multipart upload: %w", err)
	}
	return imur.UploadID, nil
}

func NewOSSClient(obs stgmod.ObjectStorage) *OSSClient {
	// 创建OSSClient实例。
	client, err := oss.New(obs.Endpoint, obs.AK, obs.SK)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	bucket, err := client.Bucket(obs.Bucket)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	return &OSSClient{
		client: client,
		bucket: bucket,
	}
}

func (c *OSSClient) UploadPart() {

}

func (c *OSSClient) CompleteMultipartUpload() (string, error) {
	return "", nil
}

func (c *OSSClient) AbortMultipartUpload() {

}

func (c *OSSClient) Close() {
	// 关闭client

}
