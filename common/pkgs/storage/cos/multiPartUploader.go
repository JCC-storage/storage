package cos

import (
	"context"
	"fmt"
	"github.com/tencentyun/cos-go-sdk-v5"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"io"
	"net/http"
	"net/url"
)

type MultiPartUploader struct {
	client *cos.Client
}

func NewMultiPartUpload(address *cdssdk.COSAddress) *MultiPartUploader {
	// cos的endpoint已包含bucket名，会自动将桶解析出来
	u, _ := url.Parse(address.Endpoint)
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  address.AK,
			SecretKey: address.SK,
		},
	})

	return &MultiPartUploader{
		client: client,
	}
}

func (c *MultiPartUploader) InitiateMultipartUpload(objectName string) (string, error) {
	v, _, err := c.client.Object.InitiateMultipartUpload(context.Background(), objectName, nil)
	if err != nil {
		log.Error("Failed to initiate multipart upload: %v", err)
		return "", err
	}
	return v.UploadID, nil
}

func (c *MultiPartUploader) UploadPart(uploadID string, key string, partSize int64, partNumber int, stream io.Reader) (*types.UploadPartOutput, error) {
	resp, err := c.client.Object.UploadPart(
		context.Background(), key, uploadID, partNumber, stream, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to upload part: %w", err)
	}

	result := &types.UploadPartOutput{
		ETag:       resp.Header.Get("ETag"),
		PartNumber: partNumber,
	}
	return result, nil
}

func (c *MultiPartUploader) CompleteMultipartUpload(uploadID string, key string, parts []*types.UploadPartOutput) error {
	opt := &cos.CompleteMultipartUploadOptions{}
	for i := 0; i < len(parts); i++ {
		opt.Parts = append(opt.Parts, cos.Object{
			PartNumber: parts[i].PartNumber, ETag: parts[i].ETag},
		)
	}
	_, _, err := c.client.Object.CompleteMultipartUpload(
		context.Background(), key, uploadID, opt,
	)
	if err != nil {
		return err
	}

	return nil
}
func (c *MultiPartUploader) AbortMultipartUpload() {

}

func (c *MultiPartUploader) Close() {

}
