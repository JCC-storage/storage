package obs

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

func Test_S2S(t *testing.T) {
	Convey("OBS", t, func() {
		s2s := S2STransfer{
			dstStg: &cdssdk.OBSType{
				Region:    "cn-north-4",
				Endpoint:  "obs.cn-north-4.myhuaweicloud.com",
				AK:        "",
				SK:        "",
				Bucket:    "pcm3-bucket3",
				ProjectID: "",
			},
			feat: &cdssdk.S2STransferFeature{
				TempDir: "s2s",
			},
		}

		newPath, err := s2s.Transfer(context.TODO(), stgmod.StorageDetail{
			Storage: cdssdk.Storage{
				Type: &cdssdk.OBSType{
					Region:    "cn-north-4",
					Endpoint:  "obs.cn-north-4.myhuaweicloud.com",
					AK:        "",
					SK:        "",
					Bucket:    "pcm2-bucket2",
					ProjectID: "",
				},
			},
		}, "test_data/test03.txt")
		defer s2s.Abort()

		So(err, ShouldEqual, nil)
		t.Logf("newPath: %s", newPath)
	})
}
