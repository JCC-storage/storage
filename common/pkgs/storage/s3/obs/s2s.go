package obs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/basic"
	oms "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/oms/v2"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/oms/v2/model"
	omsregion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/oms/v2/region"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/os2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/s3/utils"
)

type S2STransfer struct {
	dstStg *cdssdk.OBSType
	feat   *cdssdk.S2STransferFeature
	taskID *int64
	omsCli *oms.OmsClient
}

func NewS2STransfer(dstStg *cdssdk.OBSType, feat *cdssdk.S2STransferFeature) *S2STransfer {
	return &S2STransfer{
		dstStg: dstStg,
		feat:   feat,
	}
}

// 判断是否能从指定的源存储中直传到当前存储的目的路径
func (s *S2STransfer) CanTransfer(src stgmod.StorageDetail) bool {
	req := s.makeRequest(src.Storage.Type, "")
	return req != nil
}

// 执行数据直传。返回传输后的文件路径
func (s *S2STransfer) Transfer(ctx context.Context, src stgmod.StorageDetail, srcPath string) (string, error) {
	req := s.makeRequest(src.Storage.Type, srcPath)
	if req == nil {
		return "", fmt.Errorf("unsupported source storage type: %T", src.Storage.Type)
	}

	auth, err := basic.NewCredentialsBuilder().
		WithAk(s.dstStg.AK).
		WithSk(s.dstStg.SK).
		WithProjectId(s.dstStg.ProjectID).
		SafeBuild()
	if err != nil {
		return "", err
	}

	region, err := omsregion.SafeValueOf(s.dstStg.Region)
	if err != nil {
		return "", err
	}

	cli, err := oms.OmsClientBuilder().
		WithRegion(region).
		WithCredential(auth).
		SafeBuild()
	if err != nil {
		return "", err
	}

	tempPrefix := utils.JoinKey(s.feat.TempDir, os2.GenerateRandomFileName(10)) + "/"

	taskType := model.GetCreateTaskReqTaskTypeEnum().OBJECT
	s.omsCli = oms.NewOmsClient(cli)
	resp, err := s.omsCli.CreateTask(&model.CreateTaskRequest{
		Body: &model.CreateTaskReq{
			TaskType: &taskType,
			SrcNode:  req,
			DstNode: &model.DstNodeReq{
				Region:     s.dstStg.Region,
				Ak:         s.dstStg.AK,
				Sk:         s.dstStg.SK,
				Bucket:     s.dstStg.Bucket,
				SavePrefix: &tempPrefix,
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("create task: %w", err)
	}

	s.taskID = resp.Id

	err = s.waitTask(ctx, *resp.Id)
	if err != nil {
		return "", fmt.Errorf("wait task: %w", err)
	}

	return utils.JoinKey(tempPrefix, srcPath), nil
}

func (s *S2STransfer) makeRequest(srcStg cdssdk.StorageType, srcPath string) *model.SrcNodeReq {
	switch srcStg := srcStg.(type) {
	case *cdssdk.OBSType:
		cloudType := "HuaweiCloud"
		return &model.SrcNodeReq{
			CloudType: &cloudType,
			Region:    &srcStg.Region,
			Ak:        &srcStg.AK,
			Sk:        &srcStg.SK,
			Bucket:    &srcStg.Bucket,
			ObjectKey: &[]string{srcPath},
		}

	default:
		return nil
	}
}

func (s *S2STransfer) waitTask(ctx context.Context, taskId int64) error {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	failures := 0

	for {
		resp, err := s.omsCli.ShowTask(&model.ShowTaskRequest{
			TaskId: fmt.Sprintf("%v", taskId),
		})
		if err != nil {
			if failures < 3 {
				failures++
				continue
			}

			return fmt.Errorf("show task failed too many times: %w", err)
		}
		failures = 0

		if *resp.Status == 3 {
			return fmt.Errorf("task stopped")
		}

		if *resp.Status == 4 {
			return errors.New(resp.ErrorReason.String())
		}

		if *resp.Status == 5 {
			return nil
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// 完成传输
func (s *S2STransfer) Complete() {

}

// 取消传输。如果已经调用了Complete，则这个方法应该无效果
func (s *S2STransfer) Abort() {
	if s.taskID != nil {
		s.omsCli.StopTask(&model.StopTaskRequest{
			TaskId: fmt.Sprintf("%v", *s.taskID),
		})

		s.omsCli.DeleteTask(&model.DeleteTaskRequest{
			TaskId: fmt.Sprintf("%v", *s.taskID),
		})
	}
}
