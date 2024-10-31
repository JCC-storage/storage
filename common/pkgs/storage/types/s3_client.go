package types

import (
	"fmt"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/obs"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/oss"
)

//type ObjectStorageInfo interface {
//	NewClient() (ObjectStorageClient, error)
//}

type ObjectStorageClient interface {
	InitiateMultipartUpload(objectName string) (string, error)
	UploadPart()
	CompleteMultipartUpload() (string, error)
	AbortMultipartUpload()
	Close()
}

func NewObjectStorageClient(info stgmod.ObjectStorage) (ObjectStorageClient, error) {
	switch info.Manufacturer {
	case stgmod.AliCloud:
		return oss.NewOSSClient(info), nil
	case stgmod.HuaweiCloud:
		return &obs.OBSClient{}, nil
	}
	return nil, fmt.Errorf("unknown cloud storage manufacturer %s", info.Manufacturer)
}
