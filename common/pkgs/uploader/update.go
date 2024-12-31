package uploader

import (
	"context"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type UpdateUploader struct {
	uploader   *Uploader
	pkgID      cdssdk.PackageID
	targetStg  stgmod.StorageDetail
	distMutex  *distlock.Mutex
	loadToStgs []stgmod.StorageDetail
	loadToPath []string
	successes  []coormq.AddObjectEntry
	lock       sync.Mutex
	commited   bool
}

type UploadStorageInfo struct {
	Storage        stgmod.StorageDetail
	Delay          time.Duration
	IsSameLocation bool
}

type UpdateResult struct {
	// 上传成功的文件列表，Key为Path
	Objects map[string]cdssdk.Object
}

func (w *UpdateUploader) Upload(pat string, size int64, stream io.Reader) error {
	uploadTime := time.Now()

	ft := ioswitch2.NewFromTo()
	fromExec, hd := ioswitch2.NewFromDriver(ioswitch2.RawStream())
	ft.AddFrom(fromExec).
		AddTo(ioswitch2.NewToShardStore(*w.targetStg.MasterHub, w.targetStg, ioswitch2.RawStream(), "fileHash"))

	for i, stg := range w.loadToStgs {
		ft.AddTo(ioswitch2.NewLoadToShared(*stg.MasterHub, stg.Storage, path.Join(w.loadToPath[i], pat)))
	}

	plans := exec.NewPlanBuilder()
	err := parser.Parse(ft, plans)
	if err != nil {
		return fmt.Errorf("parsing plan: %w", err)
	}

	exeCtx := exec.NewExecContext()
	exec.SetValueByType(exeCtx, w.uploader.stgAgts)
	exec := plans.Execute(exeCtx)
	exec.BeginWrite(io.NopCloser(stream), hd)
	ret, err := exec.Wait(context.TODO())
	if err != nil {
		return fmt.Errorf("executing plan: %w", err)
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	// 记录上传结果
	w.successes = append(w.successes, coormq.AddObjectEntry{
		Path:       pat,
		Size:       size,
		FileHash:   ret["fileHash"].(*ops2.FileHashValue).Hash,
		UploadTime: uploadTime,
		StorageIDs: []cdssdk.StorageID{w.targetStg.Storage.StorageID},
	})
	return nil
}

func (w *UpdateUploader) Commit() (UpdateResult, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.commited {
		return UpdateResult{}, fmt.Errorf("package already commited")
	}
	w.commited = true

	defer w.distMutex.Unlock()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return UpdateResult{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	updateResp, err := coorCli.UpdatePackage(coormq.NewUpdatePackage(w.pkgID, w.successes, nil))
	if err != nil {
		return UpdateResult{}, fmt.Errorf("updating package: %w", err)
	}

	ret := UpdateResult{
		Objects: make(map[string]cdssdk.Object),
	}

	for _, entry := range updateResp.Added {
		ret.Objects[entry.Path] = entry
	}

	return ret, nil
}

func (w *UpdateUploader) Abort() {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.commited {
		return
	}

	w.commited = true
	w.distMutex.Unlock()
}
