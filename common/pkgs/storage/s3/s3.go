package s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory/reg"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/s3/obs"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

func init() {
	reg.RegisterBuilder[*cdssdk.COSType](newBuilder)
	reg.RegisterBuilder[*cdssdk.OSSType](newBuilder)
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
	agt := &Agent{
		Detail: b.detail,
	}

	if b.detail.Storage.ShardStore != nil {
		cfg, ok := b.detail.Storage.ShardStore.(*cdssdk.S3ShardStorage)
		if !ok {
			return nil, fmt.Errorf("invalid shard store type %T for local storage", b.detail.Storage.ShardStore)
		}

		cli, bkt, err := createS3Client(b.detail.Storage.Type)
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

func (b *builder) ShardStoreDesc() types.ShardStoreDesc {
	return &ShardStoreDesc{builder: b}
}

func (b *builder) PublicStoreDesc() types.PublicStoreDesc {
	return &PublicStoreDesc{}
}

func (b *builder) CreateMultiparter() (types.Multiparter, error) {
	feat := utils.FindFeature[*cdssdk.MultipartUploadFeature](b.detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.MultipartUploadFeature{})
	}

	return &Multiparter{
		detail: b.detail,
		feat:   feat,
	}, nil
}

func (b *builder) CreateS2STransfer() (types.S2STransfer, error) {
	feat := utils.FindFeature[*cdssdk.S2STransferFeature](b.detail)
	if feat == nil {
		return nil, fmt.Errorf("feature %T not found", cdssdk.S2STransferFeature{})
	}

	switch addr := b.detail.Storage.Type.(type) {
	case *cdssdk.OBSType:
		return obs.NewS2STransfer(addr, feat), nil
	default:
		return nil, fmt.Errorf("unsupported storage type %T", addr)
	}
}

func createS3Client(addr cdssdk.StorageType) (*s3.Client, string, error) {
	switch addr := addr.(type) {
	// case *cdssdk.COSType:

	// case *cdssdk.OSSType:

	case *cdssdk.OBSType:
		return obs.CreateS2Client(addr)

	default:
		return nil, "", fmt.Errorf("unsupported storage type %T", addr)
	}
}
