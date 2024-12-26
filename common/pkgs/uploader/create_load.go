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

type CreateLoadUploader struct {
	pkg        cdssdk.Package
	userID     cdssdk.UserID
	targetStgs []stgmod.StorageDetail
	loadRoots  []string
	uploader   *Uploader
	distlock   *distlock.Mutex
	successes  []coormq.AddObjectEntry
	lock       sync.Mutex
	commited   bool
}

type CreateLoadResult struct {
	Package cdssdk.Package
	Objects map[string]cdssdk.Object
}

func (u *CreateLoadUploader) Upload(pa string, size int64, stream io.Reader) error {
	uploadTime := time.Now()
	stgIDs := make([]cdssdk.StorageID, 0, len(u.targetStgs))

	ft := ioswitch2.FromTo{}
	fromExec, hd := ioswitch2.NewFromDriver(ioswitch2.RawStream())
	ft.AddFrom(fromExec)
	for i, stg := range u.targetStgs {
		ft.AddTo(ioswitch2.NewToShardStore(*stg.MasterHub, stg, ioswitch2.RawStream(), "fileHash"))
		ft.AddTo(ioswitch2.NewLoadToShared(*stg.MasterHub, stg.Storage, path.Join(u.loadRoots[i], pa)))
		stgIDs = append(stgIDs, stg.Storage.StorageID)
	}

	plans := exec.NewPlanBuilder()
	err := parser.Parse(ft, plans)
	if err != nil {
		return fmt.Errorf("parsing plan: %w", err)
	}

	exeCtx := exec.NewExecContext()
	exec.SetValueByType(exeCtx, u.uploader.stgMgr)
	exec := plans.Execute(exeCtx)
	exec.BeginWrite(io.NopCloser(stream), hd)
	ret, err := exec.Wait(context.TODO())
	if err != nil {
		return fmt.Errorf("executing plan: %w", err)
	}

	u.lock.Lock()
	defer u.lock.Unlock()

	// 记录上传结果
	fileHash := ret["fileHash"].(*ops2.FileHashValue).Hash
	u.successes = append(u.successes, coormq.AddObjectEntry{
		Path:       pa,
		Size:       size,
		FileHash:   fileHash,
		UploadTime: uploadTime,
		StorageIDs: stgIDs,
	})
	return nil
}

func (u *CreateLoadUploader) Commit() (CreateLoadResult, error) {
	u.lock.Lock()
	defer u.lock.Unlock()

	if u.commited {
		return CreateLoadResult{}, fmt.Errorf("package already commited")
	}
	u.commited = true

	defer u.distlock.Unlock()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return CreateLoadResult{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	updateResp, err := coorCli.UpdatePackage(coormq.NewUpdatePackage(u.pkg.PackageID, u.successes, nil))
	if err != nil {
		return CreateLoadResult{}, fmt.Errorf("updating package: %w", err)
	}

	ret := CreateLoadResult{
		Package: u.pkg,
		Objects: make(map[string]cdssdk.Object),
	}

	for _, entry := range updateResp.Added {
		ret.Objects[entry.Path] = entry
	}

	for i, stg := range u.targetStgs {
		// 不关注是否成功
		coorCli.StoragePackageLoaded(coormq.ReqStoragePackageLoaded(u.userID, stg.Storage.StorageID, u.pkg.PackageID, u.loadRoots[i], nil))
	}

	return ret, nil
}

func (u *CreateLoadUploader) Abort() {
	u.lock.Lock()
	defer u.lock.Unlock()

	if u.commited {
		return
	}
	u.commited = true

	u.distlock.Unlock()

	// TODO 可以考虑删除PackageID
}
