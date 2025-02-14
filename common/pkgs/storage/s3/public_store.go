package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type PublicStoreDesc struct {
	types.EmptyPublicStoreDesc
	Detail *stgmod.StorageDetail
}

func NewPublicStoreDesc(detail *stgmod.StorageDetail) PublicStoreDesc {
	return PublicStoreDesc{
		Detail: detail,
	}
}

func (d *PublicStoreDesc) Enabled() bool {
	return d.Detail.Storage.PublicStore != nil
}

type PublicStore struct {
	Detail stgmod.StorageDetail
	Bucket string
	cli    *s3.Client
	cfg    cdssdk.S3PublicStorage
}

func NewPublicStore(detail stgmod.StorageDetail, cli *s3.Client, bkt string, cfg cdssdk.S3PublicStorage) (*PublicStore, error) {
	return &PublicStore{
		Detail: detail,
		Bucket: bkt,
		cli:    cli,
		cfg:    cfg,
	}, nil
}

func (s *PublicStore) Start(ch *types.StorageEventChan) {
	s.getLogger().Infof("component start, LoadBase: %v", s.cfg.LoadBase)
}

func (s *PublicStore) Stop() {
	s.getLogger().Infof("component stop")
}

func (s *PublicStore) Write(objPath string, stream io.Reader) error {
	key := JoinKey(objPath, s.cfg.LoadBase)

	_, err := s.cli.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   stream,
	})

	return err
}

func (s *PublicStore) getLogger() logger.Logger {
	return logger.WithField("PublicStore", "S3").WithField("Storage", s.Detail.Storage.String())
}
