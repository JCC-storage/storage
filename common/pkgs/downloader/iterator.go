package downloader

import (
	"context"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type downloadStorageInfo struct {
	Storage      stgmod.StorageDetail
	ObjectPinned bool
	Blocks       []stgmod.ObjectBlock
	Distance     float64
}

type DownloadContext struct {
	Distlock *distlock.Service
}
type DownloadObjectIterator struct {
	OnClosing func()

	downloader   *Downloader
	reqs         []downloadReqeust2
	currentIndex int
	inited       bool

	coorCli     *coormq.Client
	allStorages map[cdssdk.StorageID]stgmod.StorageDetail
}

func NewDownloadObjectIterator(downloader *Downloader, downloadObjs []downloadReqeust2) *DownloadObjectIterator {
	return &DownloadObjectIterator{
		downloader: downloader,
		reqs:       downloadObjs,
	}
}

func (i *DownloadObjectIterator) MoveNext() (*Downloading, error) {
	if !i.inited {
		if err := i.init(); err != nil {
			return nil, err
		}

		i.inited = true
	}

	if i.currentIndex >= len(i.reqs) {
		return nil, iterator.ErrNoMoreItem
	}

	item, err := i.doMove()
	i.currentIndex++
	return item, err
}

func (i *DownloadObjectIterator) init() error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	i.coorCli = coorCli

	allStgIDsMp := make(map[cdssdk.StorageID]bool)
	for _, obj := range i.reqs {
		if obj.Detail == nil {
			continue
		}

		for _, p := range obj.Detail.PinnedAt {
			allStgIDsMp[p] = true
		}

		for _, b := range obj.Detail.Blocks {
			allStgIDsMp[b.StorageID] = true
		}
	}

	stgIDs := lo.Keys(allStgIDsMp)
	getStgs, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails(stgIDs))
	if err != nil {
		return fmt.Errorf("getting storage details: %w", err)
	}

	i.allStorages = make(map[cdssdk.StorageID]stgmod.StorageDetail)
	for idx, s := range getStgs.Storages {
		if s == nil {
			return fmt.Errorf("storage %v not found", stgIDs[idx])
		}
		if s.Shard == nil {
			return fmt.Errorf("storage %v has no shard store", stgIDs[idx])
		}

		i.allStorages[s.Storage.StorageID] = *s
	}

	return nil
}

func (iter *DownloadObjectIterator) doMove() (*Downloading, error) {
	req := iter.reqs[iter.currentIndex]
	if req.Detail == nil {
		return &Downloading{
			Object:  nil,
			File:    nil,
			Request: req.Raw,
		}, nil
	}

	switch red := req.Detail.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		reader, err := iter.downloadNoneOrRepObject(req)
		if err != nil {
			return nil, fmt.Errorf("downloading object %v: %w", req.Raw.ObjectID, err)
		}

		return &Downloading{
			Object:  &req.Detail.Object,
			File:    reader,
			Request: req.Raw,
		}, nil

	case *cdssdk.RepRedundancy:
		reader, err := iter.downloadNoneOrRepObject(req)
		if err != nil {
			return nil, fmt.Errorf("downloading rep object %v: %w", req.Raw.ObjectID, err)
		}

		return &Downloading{
			Object:  &req.Detail.Object,
			File:    reader,
			Request: req.Raw,
		}, nil

	case *cdssdk.ECRedundancy:
		reader, err := iter.downloadECObject(req, red)
		if err != nil {
			return nil, fmt.Errorf("downloading ec object %v: %w", req.Raw.ObjectID, err)
		}

		return &Downloading{
			Object:  &req.Detail.Object,
			File:    reader,
			Request: req.Raw,
		}, nil

	case *cdssdk.LRCRedundancy:
		reader, err := iter.downloadLRCObject(req, red)
		if err != nil {
			return nil, fmt.Errorf("downloading lrc object %v: %w", req.Raw.ObjectID, err)
		}

		return &Downloading{
			Object:  &req.Detail.Object,
			File:    reader,
			Request: req.Raw,
		}, nil
	}

	return nil, fmt.Errorf("unsupported redundancy type: %v of object %v", reflect.TypeOf(req.Detail.Object.Redundancy), req.Raw.ObjectID)
}

func (i *DownloadObjectIterator) Close() {
	if i.OnClosing != nil {
		i.OnClosing()
	}
}

func (iter *DownloadObjectIterator) downloadNoneOrRepObject(obj downloadReqeust2) (io.ReadCloser, error) {
	allStgs, err := iter.sortDownloadStorages(obj)
	if err != nil {
		return nil, err
	}

	bsc, blocks := iter.getMinReadingBlockSolution(allStgs, 1)
	osc, stg := iter.getMinReadingObjectSolution(allStgs, 1)
	if bsc < osc {
		logger.Debugf("downloading object %v from storage %v", obj.Raw.ObjectID, blocks[0].Storage.Storage)
		return iter.downloadFromStorage(&blocks[0].Storage, obj)
	}

	if osc == math.MaxFloat64 {
		// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
		return nil, fmt.Errorf("no storage has this object")
	}

	logger.Debugf("downloading object %v from storage %v", obj.Raw.ObjectID, stg)
	return iter.downloadFromStorage(stg, obj)
}

func (iter *DownloadObjectIterator) downloadECObject(req downloadReqeust2, ecRed *cdssdk.ECRedundancy) (io.ReadCloser, error) {
	allNodes, err := iter.sortDownloadStorages(req)
	if err != nil {
		return nil, err
	}

	bsc, blocks := iter.getMinReadingBlockSolution(allNodes, ecRed.K)
	osc, stg := iter.getMinReadingObjectSolution(allNodes, ecRed.K)

	if bsc < osc {
		var logStrs []any = []any{fmt.Sprintf("downloading ec object %v from blocks: ", req.Raw.ObjectID)}
		for i, b := range blocks {
			if i > 0 {
				logStrs = append(logStrs, ", ")
			}
			logStrs = append(logStrs, fmt.Sprintf("%v@%v", b.Block.Index, b.Storage.Storage.String()))
		}
		logger.Debug(logStrs...)

		pr, pw := io.Pipe()
		go func() {
			readPos := req.Raw.Offset
			totalReadLen := req.Detail.Object.Size - req.Raw.Offset
			if req.Raw.Length >= 0 {
				totalReadLen = math2.Min(req.Raw.Length, totalReadLen)
			}

			firstStripIndex := readPos / ecRed.StripSize()
			stripIter := NewStripIterator(iter.downloader, req.Detail.Object, blocks, ecRed, firstStripIndex, iter.downloader.strips, iter.downloader.cfg.ECStripPrefetchCount)
			defer stripIter.Close()

			for totalReadLen > 0 {
				strip, err := stripIter.MoveNext()
				if err == iterator.ErrNoMoreItem {
					pw.CloseWithError(io.ErrUnexpectedEOF)
					return
				}
				if err != nil {
					pw.CloseWithError(err)
					return
				}

				readRelativePos := readPos - strip.Position
				curReadLen := math2.Min(totalReadLen, ecRed.StripSize()-readRelativePos)

				err = io2.WriteAll(pw, strip.Data[readRelativePos:readRelativePos+curReadLen])
				if err != nil {
					pw.CloseWithError(err)
					return
				}

				totalReadLen -= curReadLen
				readPos += curReadLen
			}
			pw.Close()
		}()

		return pr, nil
	}

	// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
	if osc == math.MaxFloat64 {
		return nil, fmt.Errorf("no enough blocks to reconstruct the object %v , want %d, get only %d", req.Raw.ObjectID, ecRed.K, len(blocks))
	}

	logger.Debugf("downloading ec object %v from storage %v", req.Raw.ObjectID, stg)
	return iter.downloadFromStorage(stg, req)
}

func (iter *DownloadObjectIterator) sortDownloadStorages(req downloadReqeust2) ([]*downloadStorageInfo, error) {
	var stgIDs []cdssdk.StorageID
	for _, id := range req.Detail.PinnedAt {
		if !lo.Contains(stgIDs, id) {
			stgIDs = append(stgIDs, id)
		}
	}
	for _, b := range req.Detail.Blocks {
		if !lo.Contains(stgIDs, b.StorageID) {
			stgIDs = append(stgIDs, b.StorageID)
		}
	}

	downloadNodeMap := make(map[cdssdk.StorageID]*downloadStorageInfo)
	for _, id := range req.Detail.PinnedAt {
		node, ok := downloadNodeMap[id]
		if !ok {
			mod := iter.allStorages[id]
			node = &downloadStorageInfo{
				Storage:      mod,
				ObjectPinned: true,
				Distance:     iter.getNodeDistance(mod),
			}
			downloadNodeMap[id] = node
		}

		node.ObjectPinned = true
	}

	for _, b := range req.Detail.Blocks {
		node, ok := downloadNodeMap[b.StorageID]
		if !ok {
			mod := iter.allStorages[b.StorageID]
			node = &downloadStorageInfo{
				Storage:  mod,
				Distance: iter.getNodeDistance(mod),
			}
			downloadNodeMap[b.StorageID] = node
		}

		node.Blocks = append(node.Blocks, b)
	}

	return sort2.Sort(lo.Values(downloadNodeMap), func(left, right *downloadStorageInfo) int {
		return sort2.Cmp(left.Distance, right.Distance)
	}), nil
}

func (iter *DownloadObjectIterator) getMinReadingBlockSolution(sortedStgs []*downloadStorageInfo, k int) (float64, []downloadBlock) {
	gotBlocksMap := bitmap.Bitmap64(0)
	var gotBlocks []downloadBlock
	dist := float64(0.0)
	for _, n := range sortedStgs {
		for _, b := range n.Blocks {
			if !gotBlocksMap.Get(b.Index) {
				gotBlocks = append(gotBlocks, downloadBlock{
					Storage: n.Storage,
					Block:   b,
				})
				gotBlocksMap.Set(b.Index, true)
				dist += n.Distance
			}

			if len(gotBlocks) >= k {
				return dist, gotBlocks
			}
		}
	}

	return math.MaxFloat64, gotBlocks
}

func (iter *DownloadObjectIterator) getMinReadingObjectSolution(sortedStgs []*downloadStorageInfo, k int) (float64, *stgmod.StorageDetail) {
	dist := math.MaxFloat64
	var downloadStg *stgmod.StorageDetail
	for _, n := range sortedStgs {
		if n.ObjectPinned && float64(k)*n.Distance < dist {
			dist = float64(k) * n.Distance
			stg := n.Storage
			downloadStg = &stg
		}
	}

	return dist, downloadStg
}

func (iter *DownloadObjectIterator) getNodeDistance(stg stgmod.StorageDetail) float64 {
	if stgglb.Local.NodeID != nil {
		if stg.MasterHub.NodeID == *stgglb.Local.NodeID {
			return consts.NodeDistanceSameNode
		}
	}

	if stg.MasterHub.LocationID == stgglb.Local.LocationID {
		return consts.NodeDistanceSameLocation
	}

	c := iter.downloader.conn.Get(stg.MasterHub.NodeID)
	if c == nil || c.Delay == nil || *c.Delay > time.Duration(float64(time.Millisecond)*iter.downloader.cfg.HighLatencyNodeMs) {
		return consts.NodeDistanceHighLatencyNode
	}

	return consts.NodeDistanceOther
}

func (iter *DownloadObjectIterator) downloadFromStorage(stg *stgmod.StorageDetail, req downloadReqeust2) (io.ReadCloser, error) {
	var strHandle *exec.DriverReadStream
	ft := ioswitch2.NewFromTo()

	toExec, handle := ioswitch2.NewToDriver(-1)
	toExec.Range = exec.Range{
		Offset: req.Raw.Offset,
	}
	if req.Raw.Length != -1 {
		len := req.Raw.Length
		toExec.Range.Length = &len
	}

	ft.AddFrom(ioswitch2.NewFromShardstore(req.Detail.Object.FileHash, *stg.MasterHub, stg.Storage, -1)).AddTo(toExec)
	strHandle = handle

	parser := parser.NewParser(cdssdk.DefaultECRedundancy)
	plans := exec.NewPlanBuilder()
	if err := parser.Parse(ft, plans); err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	exeCtx := exec.NewExecContext()
	exec.SetValueByType(exeCtx, iter.downloader.stgMgr)
	exec := plans.Execute(exeCtx)
	go exec.Wait(context.TODO())

	return exec.BeginRead(strHandle)
}
