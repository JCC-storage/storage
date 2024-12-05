package s3

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

func init() {
	reg.RegisterBuilder[*cdssdk.COSType](createService, createComponent)
	reg.RegisterBuilder[*cdssdk.OSSType](createService, createComponent)
	reg.RegisterBuilder[*cdssdk.OBSType](createService, createComponent)
}

func createService(detail stgmod.StorageDetail) (types.StorageService, error) {
	svc := &Service{
		Detail: detail,
	}

	if detail.Storage.ShardStore != nil {
		cfg, ok := detail.Storage.ShardStore.(*cdssdk.S3ShardStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shard store type %T for local storage", detail.Storage.ShardStore)
		}

		cli, bkt, err := createS3Client(detail.Storage.Type)
		if err != nil {
			return nil, err
		}

		store, err := NewShardStore(svc, cli, bkt, *cfg)
		if err != nil {
			return nil, err
		}

		svc.ShardStore = store
	}

	return svc, nil
}

func createComponent(detail stgmod.StorageDetail, typ reflect.Type) (any, error) {
	switch typ {
	case reflect2.TypeOf[types.MultipartInitiator]():
		feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](detail)
		if feat == nil {
			return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
		}

		cli, bkt, err := createS3Client(detail.Storage.Type)
		if err != nil {
			return nil, err
		}

		return &MultipartInitiator{
			cli:     cli,
			bucket:  bkt,
			tempDir: feat.TempDir,
		}, nil

	case reflect2.TypeOf[types.MultipartUploader]():
		feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](detail)
		if feat == nil {
			return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
		}

		cli, bkt, err := createS3Client(detail.Storage.Type)
		if err != nil {
			return nil, err
		}

		return &MultipartUploader{
			cli:    cli,
			bucket: bkt,
		}, nil
	}

	return nil, fmt.Errorf("unsupported component type %v", typ)
}

func createS3Client(addr cdssdk.StorageType) (*s3.Client, string, error) {
	switch addr := addr.(type) {
	// case *cdssdk.COSType:

	// case *cdssdk.OSSType:

	case *cdssdk.OBSType:
		awsConfig := aws.Config{}

		cre := aws.Credentials{
			AccessKeyID:     addr.AK,
			SecretAccessKey: addr.SK,
		}
		awsConfig.Credentials = &credentials.StaticCredentialsProvider{Value: cre}
		awsConfig.Region = addr.Region

		options := []func(*s3.Options){}
		options = append(options, func(s3Opt *s3.Options) {
			s3Opt.BaseEndpoint = &addr.Endpoint
		})

		cli := s3.NewFromConfig(awsConfig, options...)
		return cli, addr.Bucket, nil

	default:
		return nil, "", fmt.Errorf("unsupported storage type %T", addr)
	}
}
