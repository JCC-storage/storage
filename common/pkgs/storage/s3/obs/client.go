package obs

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func CreateS2Client(addr *cdssdk.OBSType) (*s3.Client, string, error) {
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
