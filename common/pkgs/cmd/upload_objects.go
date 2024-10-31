package cmd

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/mgr"
)

type UploadObjects struct {
	userID       cdssdk.UserID
	packageID    cdssdk.PackageID
	objectIter   iterator.UploadingObjectIterator
	nodeAffinity *cdssdk.NodeID
}

type UploadObjectsResult struct {
	Objects []ObjectUploadResult
}

type ObjectUploadResult struct {
	Info   *iterator.IterUploadingObject
	Error  error
	Object cdssdk.Object
}

type UploadStorageInfo struct {
	Storage        stgmod.StorageDetail
	Delay          time.Duration
	IsSameLocation bool
}

type UploadObjectsContext struct {
	Distlock     *distlock.Service
	Connectivity *connectivity.Collector
	StgMgr       *mgr.Manager
}

func NewUploadObjects(userID cdssdk.UserID, packageID cdssdk.PackageID, objIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) *UploadObjects {
	return &UploadObjects{
		userID:       userID,
		packageID:    packageID,
		objectIter:   objIter,
		nodeAffinity: nodeAffinity,
	}
}

func (t *UploadObjects) Execute(ctx *UploadObjectsContext) (*UploadObjectsResult, error) {
	defer t.objectIter.Close()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	getUserStgsResp, err := coorCli.GetUserStorageDetails(coormq.ReqGetUserStorageDetails(t.userID))
	if err != nil {
		return nil, fmt.Errorf("getting user nodes: %w", err)
	}

	cons := ctx.Connectivity.GetAll()
	var userStgs []UploadStorageInfo
	for _, stg := range getUserStgsResp.Storages {
		if stg.MasterHub == nil {
			continue
		}

		delay := time.Duration(math.MaxInt64)

		con, ok := cons[stg.MasterHub.NodeID]
		if ok && con.Delay != nil {
			delay = *con.Delay
		}

		userStgs = append(userStgs, UploadStorageInfo{
			Storage:        stg,
			Delay:          delay,
			IsSameLocation: stg.MasterHub.LocationID == stgglb.Local.LocationID,
		})
	}

	if len(userStgs) == 0 {
		return nil, fmt.Errorf("user no available nodes")
	}

	// 给上传节点的IPFS加锁
	ipfsReqBlder := reqbuilder.NewBuilder()
	for _, us := range userStgs {
		ipfsReqBlder.Shard().Buzy(us.Storage.Storage.StorageID)
	}
	// TODO 考虑加Object的Create锁
	// 防止上传的副本被清除
	ipfsMutex, err := ipfsReqBlder.MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer ipfsMutex.Unlock()

	rets, err := uploadAndUpdatePackage(ctx, t.packageID, t.objectIter, userStgs, t.nodeAffinity)
	if err != nil {
		return nil, err
	}

	return &UploadObjectsResult{
		Objects: rets,
	}, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 选择设置了亲和性的节点
// 2. 从与当前客户端相同地域的节点中随机选一个
// 3. 没有的话从所有节点选择延迟最低的节点
func chooseUploadNode(nodes []UploadStorageInfo, nodeAffinity *cdssdk.NodeID) UploadStorageInfo {
	if nodeAffinity != nil {
		aff, ok := lo.Find(nodes, func(node UploadStorageInfo) bool { return node.Storage.MasterHub.NodeID == *nodeAffinity })
		if ok {
			return aff
		}
	}

	sameLocationNodes := lo.Filter(nodes, func(e UploadStorageInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationNodes) > 0 {
		return sameLocationNodes[rand.Intn(len(sameLocationNodes))]
	}

	// 选择延迟最低的节点
	nodes = sort2.Sort(nodes, func(e1, e2 UploadStorageInfo) int { return sort2.Cmp(e1.Delay, e2.Delay) })

	return nodes[0]
}

func uploadAndUpdatePackage(ctx *UploadObjectsContext, packageID cdssdk.PackageID, objectIter iterator.UploadingObjectIterator, userNodes []UploadStorageInfo, nodeAffinity *cdssdk.NodeID) ([]ObjectUploadResult, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 为所有文件选择相同的上传节点
	uploadNode := chooseUploadNode(userNodes, nodeAffinity)

	var uploadRets []ObjectUploadResult
	//上传文件夹
	var adds []coormq.AddObjectEntry
	for {
		objInfo, err := objectIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading object: %w", err)
		}
		err = func() error {
			defer objInfo.File.Close()

			uploadTime := time.Now()
			fileHash, err := uploadFile(ctx, objInfo.File, uploadNode)
			if err != nil {
				return fmt.Errorf("uploading file: %w", err)
			}

			uploadRets = append(uploadRets, ObjectUploadResult{
				Info:  objInfo,
				Error: err,
			})

			adds = append(adds, coormq.NewAddObjectEntry(objInfo.Path, objInfo.Size, fileHash, uploadTime, uploadNode.Storage.Storage.StorageID))
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	updateResp, err := coorCli.UpdatePackage(coormq.NewUpdatePackage(packageID, adds, nil))
	if err != nil {
		return nil, fmt.Errorf("updating package: %w", err)
	}

	updatedObjs := make(map[string]*cdssdk.Object)
	for _, obj := range updateResp.Added {
		o := obj
		updatedObjs[obj.Path] = &o
	}

	for i := range uploadRets {
		obj := updatedObjs[uploadRets[i].Info.Path]
		if obj == nil {
			uploadRets[i].Error = fmt.Errorf("object %s not found in package", uploadRets[i].Info.Path)
			continue
		}
		uploadRets[i].Object = *obj
	}

	return uploadRets, nil
}

func uploadFile(ctx *UploadObjectsContext, file io.Reader, uploadStg UploadStorageInfo) (cdssdk.FileHash, error) {
	ft := ioswitch2.NewFromTo()
	fromExec, hd := ioswitch2.NewFromDriver(-1)
	ft.AddFrom(fromExec).AddTo(ioswitch2.NewToShardStore(*uploadStg.Storage.MasterHub, uploadStg.Storage.Storage, -1, "fileHash"))

	parser := parser.NewParser(cdssdk.DefaultECRedundancy)
	plans := exec.NewPlanBuilder()
	err := parser.Parse(ft, plans)
	if err != nil {
		return "", fmt.Errorf("parsing plan: %w", err)
	}

	exeCtx := exec.NewExecContext()
	exec.SetValueByType(exeCtx, ctx.StgMgr)
	exec := plans.Execute(exeCtx)
	exec.BeginWrite(io.NopCloser(file), hd)
	ret, err := exec.Wait(context.TODO())
	if err != nil {
		return "", err
	}

	return ret["fileHash"].(*ops2.FileHashValue).Hash, nil
}
