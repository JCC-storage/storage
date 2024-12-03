package cos

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/tencentyun/cos-go-sdk-v5"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
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

func (c *MultiPartUploader) Initiate(objectName string) (string, error) {
	v, _, err := c.client.Object.InitiateMultipartUpload(context.Background(), objectName, nil)
	if err != nil {
		return "", fmt.Errorf("failed to initiate multipart upload: %w", err)
	}
	return v.UploadID, nil
}

func (c *MultiPartUploader) UploadPart(uploadID string, key string, partSize int64, partNumber int, stream io.Reader) (*types.UploadedPartInfo, error) {
	resp, err := c.client.Object.UploadPart(
		context.Background(), key, uploadID, partNumber, stream, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to upload part: %w", err)
	}

	result := &types.UploadedPartInfo{
		ETag:       resp.Header.Get("ETag"),
		PartNumber: partNumber,
	}
	return result, nil
}

func (c *MultiPartUploader) Complete(uploadID string, key string, parts []*types.UploadedPartInfo) error {
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
func (c *MultiPartUploader) Abort() {

}

func (c *MultiPartUploader) Close() {

}
