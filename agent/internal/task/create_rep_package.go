package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

type CreateRepPackageResult = cmd.CreateRepPackageResult

type CreateRepPackage struct {
	cmd cmd.CreateRepPackage

	Result *CreateRepPackageResult
}

func NewCreateRepPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, redundancy models.RepRedundancyInfo) *CreateRepPackage {
	return &CreateRepPackage{
		cmd: *cmd.NewCreateRepPackage(userID, bucketID, name, objIter, redundancy),
	}
}

func (t *CreateRepPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[CreateRepPackage]("Task")
	log.Debugf("begin")
	defer log.Debugf("end")

	ret, err := t.cmd.Execute(&cmd.UpdatePackageContext{
		Distlock: ctx.distlock,
	})
	t.Result = ret

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
