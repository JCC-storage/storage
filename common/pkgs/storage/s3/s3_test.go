package s3

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/smartystreets/goconvey/convey"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func Test_S3(t *testing.T) {
	Convey("OBS", t, func() {
		cli, bkt, err := createS3Client(&cdssdk.OBSType{
			Region:   "0",
			AK:       "0",
			SK:       "0",
			Endpoint: "0",
			Bucket:   "0",
		})
		So(err, ShouldEqual, nil)

		var marker *string
		for {
			resp, err := cli.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket:  aws.String(bkt),
				Prefix:  aws.String("cds"),
				MaxKeys: aws.Int32(5),
				Marker:  marker,
			})
			So(err, ShouldEqual, nil)

			fmt.Printf("\n")
			for _, obj := range resp.Contents {
				fmt.Printf("%v, %v\n", *obj.Key, *obj.LastModified)
			}

			if *resp.IsTruncated {
				marker = resp.NextMarker
			} else {
				break
			}
		}
	})
}
