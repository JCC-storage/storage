package s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

func init() {
	reg.RegisterBuilder[*cdssdk.COSType](&builder{})
	reg.RegisterBuilder[*cdssdk.OSSType](&builder{})
	reg.RegisterBuilder[*cdssdk.OBSType](&builder{})
}

type builder struct{}

func (b *builder) CreateAgent(detail stgmod.StorageDetail) (types.StorageAgent, error) {
	agt := &Agent{
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

		store, err := NewShardStore(agt, cli, bkt, *cfg, ShardStoreOption{
			// 目前对接的存储服务都不支持从上传接口直接获取到Sha256
			UseAWSSha256: false,
		})
		if err != nil {
			return nil, err
		}

		agt.ShardStore = store
	}

	return agt, nil
}

func (b *builder) CreateMultipartInitiator(detail stgmod.StorageDetail) (types.MultipartInitiator, error) {
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
}

func (b *builder) CreateMultipartUploader(detail stgmod.StorageDetail) (types.MultipartUploader, error) {
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
