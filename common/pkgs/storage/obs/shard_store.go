package obs

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/s3"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type ShardStoreDesc struct {
	s3.ShardStoreDesc
}

func (d *ShardStoreDesc) HasBypassHTTPRead() bool {
	return d.Enabled()
}

type ShardStore struct {
	*s3.ShardStore
	obsType *cdssdk.OBSType
}

func NewShardStore(detail stgmod.StorageDetail, obsType *cdssdk.OBSType, cfg cdssdk.S3ShardStorage) (*ShardStore, error) {
	sd := ShardStore{
		obsType: obsType,
	}

	s3Cli, bkt, err := createClient(obsType)
	if err != nil {
		return nil, err
	}

	sd.ShardStore, err = s3.NewShardStore(detail, s3Cli, bkt, cfg, s3.ShardStoreOption{
		UseAWSSha256: false,
	})
	if err != nil {
		return nil, err
	}

	return &sd, nil
}

func (s *ShardStore) HTTPBypassRead(fileHash cdssdk.FileHash) (types.HTTPRequest, error) {
	cli, err := obs.New(s.obsType.AK, s.obsType.SK, s.obsType.Endpoint)
	if err != nil {
		return types.HTTPRequest{}, err
	}

	getSigned, err := cli.CreateSignedUrl(&obs.CreateSignedUrlInput{
		Method:  "GET",
		Bucket:  s.Bucket,
		Key:     s.GetFilePathFromHash(fileHash),
		Expires: 3600,
	})
	if err != nil {
		return types.HTTPRequest{}, err
	}

	return types.HTTPRequest{
		URL:    getSigned.SignedUrl,
		Method: "GET",
	}, nil
}
