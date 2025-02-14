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
	reg.RegisterBuilder[*cdssdk.S3Type](newBuilder)
}

type builder struct {
	types.EmptyBuilder
	detail stgmod.StorageDetail
}

func newBuilder(detail stgmod.StorageDetail) types.StorageBuilder {
	return &builder{
		detail: detail,
	}
}

func (b *builder) CreateAgent() (types.StorageAgent, error) {
	s3Type, ok := b.detail.Storage.Type.(*cdssdk.S3Type)
	if !ok {
		return nil, fmt.Errorf("invalid storage type %T for obs agent", b.detail.Storage.Type)
	}

	agt := &Agent{
		Detail: b.detail,
	}

	if b.detail.Storage.ShardStore != nil {
		cfg, ok := b.detail.Storage.ShardStore.(*cdssdk.S3ShardStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shard store type %T for local storage", b.detail.Storage.ShardStore)
		}

		cli, bkt, err := createClient(s3Type)
		if err != nil {
			return nil, err
		}

		store, err := NewShardStore(b.detail, cli, bkt, *cfg, ShardStoreOption{UseAWSSha256: true})
		if err != nil {
			return nil, err
		}

		agt.ShardStore = store
	}

	if b.detail.Storage.PublicStore != nil {
		cfg, ok := b.detail.Storage.PublicStore.(*cdssdk.S3PublicStorage)
		if !ok {
			return nil, fmt.Errorf("invalid public store type %T for local storage", b.detail.Storage.PublicStore)
		}

		cli, bkt, err := createClient(s3Type)
		if err != nil {
			return nil, err
		}

		store, err := NewPublicStore(b.detail, cli, bkt, *cfg)
		if err != nil {
			return nil, err
		}

		agt.PublicStore = store
	}

	return agt, nil
}

func (b *builder) ShardStoreDesc() types.ShardStoreDesc {
	desc := NewShardStoreDesc(&b.detail)
	return &desc
}

func (b *builder) PublicStoreDesc() types.PublicStoreDesc {
	desc := NewPublicStoreDesc(&b.detail)
	return &desc
}

func createClient(addr *cdssdk.S3Type) (*s3.Client, string, error) {
	awsConfig := aws.Config{}

	if addr.AK != "" && addr.SK != "" {
		cre := aws.Credentials{
			AccessKeyID:     addr.AK,
			SecretAccessKey: addr.SK,
		}
		awsConfig.Credentials = &credentials.StaticCredentialsProvider{Value: cre}
	}

	awsConfig.Region = addr.Region

	options := []func(*s3.Options){}
	options = append(options, func(s3Opt *s3.Options) {
		s3Opt.BaseEndpoint = &addr.Endpoint
	})

	cli := s3.NewFromConfig(awsConfig, options...)
	return cli, addr.Bucket, nil
}

func (b *builder) CreateMultiparter() (types.Multiparter, error) {
	feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](b.detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
	}

	cli, bucket, err := createClient(b.detail.Storage.Type.(*cdssdk.S3Type))
	if err != nil {
		return nil, err
	}

	return NewMultiparter(
		b.detail,
		feat,
		bucket,
		cli,
	), nil
}
