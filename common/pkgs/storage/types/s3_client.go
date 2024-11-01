package types

import (
	"io"
)

type MultipartUploader interface {
	InitiateMultipartUpload(objectName string) (string, error)
	UploadPart(uploadID string, key string, partSize int64, partNumber int, stream io.Reader) (*UploadPartOutput, error)
	CompleteMultipartUpload(uploadID string, Key string, Parts []*UploadPartOutput) error
	AbortMultipartUpload()
	Close()
}

type UploadPartOutput struct {
	PartNumber int
	ETag       string
}
