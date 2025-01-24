package obs

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	s3stg "gitlink.org.cn/cloudream/storage/common/pkgs/storage/s3"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

func init() {
	reg.RegisterBuilder[*cdssdk.OBSType](newBuilder)
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
	obsType, ok := b.detail.Storage.Type.(*cdssdk.OBSType)
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

		store, err := NewShardStore(b.detail, obsType, *cfg)
		if err != nil {
			return nil, err
		}

		agt.ShardStore = store
	}

	return agt, nil
}

func (b *builder) ShardStoreDesc() types.ShardStoreDesc {
	return &ShardStoreDesc{
		ShardStoreDesc: s3stg.NewShardStoreDesc(&b.detail),
	}
}

func createClient(addr *cdssdk.OBSType) (*s3.Client, string, error) {
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
}

func (b *builder) CreateMultiparter() (types.Multiparter, error) {
	feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](b.detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
	}

	cli, bucket, err := createClient(b.detail.Storage.Type.(*cdssdk.OBSType))
	if err != nil {
		return nil, err
	}

	return s3stg.NewMultiparter(
		b.detail,
		feat,
		bucket,
		cli,
	), nil
}

func (b *builder) CreateS2STransfer() (types.S2STransfer, error) {
	feat := utils.FindFeature[*cdssdk.S2STransferFeature](b.detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.S2STransferFeature{})
	}

	return NewS2STransfer(b.detail.Storage.Type.(*cdssdk.OBSType), feat), nil
}
