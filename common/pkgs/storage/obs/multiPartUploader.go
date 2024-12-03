package obs

import (
	"fmt"
	"io"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type MultiPartUploader struct {
	client *obs.ObsClient
	bucket string
}

func NewMultiPartUpload(address *cdssdk.OBSAddress) *MultiPartUploader {
	client, err := obs.New(address.AK, address.SK, address.Endpoint)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	return &MultiPartUploader{
		client: client,
		bucket: address.Bucket,
	}
}

func (c *MultiPartUploader) Initiate(objectName string) (string, error) {
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = c.bucket
	input.Key = objectName
	imur, err := c.client.InitiateMultipartUpload(input)
	if err != nil {
		return "", fmt.Errorf("failed to initiate multipart upload: %w", err)
	}
	return imur.UploadId, nil
}

func (c *MultiPartUploader) UploadPart(uploadID string, key string, partSize int64, partNumber int, stream io.Reader) (*types.UploadedPartInfo, error) {
	uploadParam := &obs.UploadPartInput{
		Bucket:     c.bucket,
		Key:        key,
		UploadId:   uploadID,
		PartSize:   partSize,
		PartNumber: partNumber,
		Body:       stream,
	}

	part, err := c.client.UploadPart(uploadParam)
	if err != nil {
		return nil, fmt.Errorf("failed to upload part: %w", err)
	}
	result := &types.UploadedPartInfo{
		ETag:       part.ETag,
		PartNumber: partNumber,
	}
	return result, nil
}

func (c *MultiPartUploader) Complete(uploadID string, Key string, parts []*types.UploadedPartInfo) error {
	var uploadPart []obs.Part
	for i := 0; i < len(parts); i++ {
		uploadPart = append(uploadPart, obs.Part{
			PartNumber: parts[i].PartNumber,
			ETag:       parts[i].ETag,
		})
	}

	notifyParam := &obs.CompleteMultipartUploadInput{
		Bucket:   c.bucket,
		Key:      Key,
		UploadId: uploadID,
		Parts:    uploadPart,
	}

	_, err := c.client.CompleteMultipartUpload(notifyParam)
	if err != nil {
		return err
	}
	return nil
}
func (c *MultiPartUploader) Abort() {

}

func (c *MultiPartUploader) Close() {

}
