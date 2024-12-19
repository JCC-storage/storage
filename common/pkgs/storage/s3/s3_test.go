package s3

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/smartystreets/goconvey/convey"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func Test_S3(t *testing.T) {
	Convey("OBS", t, func() {
		cli, bkt, err := createS3Client(&cdssdk.OBSType{
			Region:   "cn-north-4",
			AK:       "CANMDYKXIWRDR0IYDB32",
			SK:       "V67yEYpu7ol2NT8nhLlNF1g9k5hq2VwIP5N5jIoQ",
			Endpoint: "https://obs.cn-north-4.myhuaweicloud.com",
			Bucket:   "pcm3-bucket3",
		})
		So(err, ShouldEqual, nil)

		// file, err := os.Open("./sky")
		So(err, ShouldEqual, nil)
		// defer file.Close()

		_, err = cli.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bkt),
			Key:    aws.String("sky2"),
			Body:   bytes.NewReader([]byte("hello world")),
			// ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
			// ContentType:       aws.String("application/octet-stream"),
			ContentLength: aws.Int64(11),
			// ContentEncoding:   aws.String("identity"),
		})

		So(err, ShouldEqual, nil)

		// var marker *string
		// for {
		// 	resp, err := cli.ListObjects(context.Background(), &s3.ListObjectsInput{
		// 		Bucket:  aws.String(bkt),
		// 		Prefix:  aws.String("cds"),
		// 		MaxKeys: aws.Int32(5),
		// 		Marker:  marker,
		// 	})
		// 	So(err, ShouldEqual, nil)

		// 	fmt.Printf("\n")
		// 	for _, obj := range resp.Contents {
		// 		fmt.Printf("%v, %v\n", *obj.Key, *obj.LastModified)
		// 	}

		// 	if *resp.IsTruncated {
		// 		marker = resp.NextMarker
		// 	} else {
		// 		break
		// 	}
		// }

	})
}

func Test_2(t *testing.T) {
	Convey("OBS", t, func() {
		dir := "d:\\Projects\\cloudream\\workspace\\storage\\common\\pkgs\\storage\\s3"
		filepath.WalkDir(dir, func(fname string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}

			info, err := d.Info()
			if err != nil {
				return nil
			}

			if info.IsDir() {
				return nil
			}

			path := strings.TrimPrefix(fname, dir+string(os.PathSeparator))
			// path := fname
			comps := strings.Split(filepath.ToSlash(path), "/")
			fmt.Println(path)
			fmt.Println(comps)
			// s.fs.syncer.SyncObject(append([]string{userName}, comps...), info.Size())
			return nil
		})
	})
}
