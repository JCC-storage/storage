package strategy

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/metacache"
)

type Request struct {
	Detail       stgmod.ObjectDetail
	Range        math2.Range
	DestHub      cdssdk.HubID      // 可以为0。此字段不为0时，DestLocation字段无意义。
	DestLocation cdssdk.LocationID // 可以为0
}

type Strategy interface {
	GetDetail() stgmod.ObjectDetail
}

// 直接下载完整对象
type DirectStrategy struct {
	Detail  stgmod.ObjectDetail
	Storage stgmod.StorageDetail
}

func (s *DirectStrategy) GetDetail() stgmod.ObjectDetail {
	return s.Detail
}

// 从指定对象重建对象
type ECReconstructStrategy struct {
	Detail     stgmod.ObjectDetail
	Redundancy cdssdk.ECRedundancy
	Blocks     []stgmod.ObjectBlock
	Storages   []stgmod.StorageDetail
}

func (s *ECReconstructStrategy) GetDetail() stgmod.ObjectDetail {
	return s.Detail
}

type LRCReconstructStrategy struct {
	Detail     stgmod.ObjectDetail
	Redundancy cdssdk.LRCRedundancy
	Blocks     []stgmod.ObjectBlock
	Storages   []stgmod.StorageDetail
}

func (s *LRCReconstructStrategy) GetDetail() stgmod.ObjectDetail {
	return s.Detail
}

type Selector struct {
	cfg          Config
	storageMeta  *metacache.StorageMeta
	hubMeta      *metacache.HubMeta
	connectivity *metacache.Connectivity
}

func NewSelector(cfg Config, storageMeta *metacache.StorageMeta, hubMeta *metacache.HubMeta, connectivity *metacache.Connectivity) *Selector {
	return &Selector{
		cfg:          cfg,
		storageMeta:  storageMeta,
		hubMeta:      hubMeta,
		connectivity: connectivity,
	}
}

func (s *Selector) Select(req Request) (Strategy, error) {
	req2 := request2{
		Detail:       req.Detail,
		Range:        req.Range,
		DestLocation: req.DestLocation,
	}

	if req.DestHub != 0 {
		req2.DestHub = s.hubMeta.Get(req.DestHub)
	}

	switch red := req.Detail.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		return s.selectForNoneOrRep(req2)

	case *cdssdk.RepRedundancy:
		return s.selectForNoneOrRep(req2)

	case *cdssdk.ECRedundancy:
		return s.selectForEC(req2, *red)

	case *cdssdk.LRCRedundancy:
		return s.selectForLRC(req2, *red)
	}

	return nil, fmt.Errorf("unsupported redundancy type: %v of object %v", reflect.TypeOf(req.Detail.Object.Redundancy), req.Detail.Object.ObjectID)
}

type downloadStorageInfo struct {
	Storage      stgmod.StorageDetail
	ObjectPinned bool
	Blocks       []stgmod.ObjectBlock
	Distance     float64
}

type downloadBlock struct {
	Storage stgmod.StorageDetail
	Block   stgmod.ObjectBlock
}

type request2 struct {
	Detail       stgmod.ObjectDetail
	Range        math2.Range
	DestHub      *cdssdk.Hub
	DestLocation cdssdk.LocationID
}

func (s *Selector) selectForNoneOrRep(req request2) (Strategy, error) {
	sortedStgs := s.sortDownloadStorages(req)
	if len(sortedStgs) == 0 {
		return nil, fmt.Errorf("no storage available for download")
	}

	_, blks := s.getMinReadingBlockSolution(sortedStgs, 1)
	if len(blks) == 0 {
		return nil, fmt.Errorf("no block available for download")
	}

	return &DirectStrategy{
		Detail:  req.Detail,
		Storage: sortedStgs[0].Storage,
	}, nil
}

func (s *Selector) selectForEC(req request2, red cdssdk.ECRedundancy) (Strategy, error) {
	sortedStgs := s.sortDownloadStorages(req)
	if len(sortedStgs) == 0 {
		return nil, fmt.Errorf("no storage available for download")
	}

	bsc, blocks := s.getMinReadingBlockSolution(sortedStgs, red.K)
	osc, stg := s.getMinReadingObjectSolution(sortedStgs, red.K)

	if bsc < osc {
		bs := make([]stgmod.ObjectBlock, len(blocks))
		ss := make([]stgmod.StorageDetail, len(blocks))
		for i, b := range blocks {
			bs[i] = b.Block
			ss[i] = b.Storage
		}

		return &ECReconstructStrategy{
			Detail:     req.Detail,
			Redundancy: red,
			Blocks:     bs,
			Storages:   ss,
		}, nil
	}

	// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
	if osc == math.MaxFloat64 {
		return nil, fmt.Errorf("no enough blocks to reconstruct the object %v , want %d, get only %d", req.Detail.Object.ObjectID, red.K, len(blocks))
	}

	return &DirectStrategy{
		Detail:  req.Detail,
		Storage: stg,
	}, nil
}

func (s *Selector) selectForLRC(req request2, red cdssdk.LRCRedundancy) (Strategy, error) {
	sortedStgs := s.sortDownloadStorages(req)
	if len(sortedStgs) == 0 {
		return nil, fmt.Errorf("no storage available for download")
	}

	var blocks []downloadBlock
	selectedBlkIdx := make(map[int]bool)
	for _, stg := range sortedStgs {
		for _, b := range stg.Blocks {
			if b.Index >= red.M() || selectedBlkIdx[b.Index] {
				continue
			}
			blocks = append(blocks, downloadBlock{
				Storage: stg.Storage,
				Block:   b,
			})
			selectedBlkIdx[b.Index] = true
		}
	}
	if len(blocks) < red.K {
		return nil, fmt.Errorf("not enough blocks to download lrc object")
	}

	bs := make([]stgmod.ObjectBlock, len(blocks))
	ss := make([]stgmod.StorageDetail, len(blocks))
	for i, b := range blocks {
		bs[i] = b.Block
		ss[i] = b.Storage
	}

	return &LRCReconstructStrategy{
		Detail:     req.Detail,
		Redundancy: red,
		Blocks:     bs,
		Storages:   ss,
	}, nil
}

func (s *Selector) sortDownloadStorages(req request2) []*downloadStorageInfo {
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

	downloadStorageMap := make(map[cdssdk.StorageID]*downloadStorageInfo)
	for _, id := range req.Detail.PinnedAt {
		storage, ok := downloadStorageMap[id]
		if !ok {
			mod := s.storageMeta.Get(id)
			if mod == nil || mod.MasterHub == nil {
				continue
			}

			storage = &downloadStorageInfo{
				Storage:      *mod,
				ObjectPinned: true,
				Distance:     s.getStorageDistance(req, *mod),
			}
			downloadStorageMap[id] = storage
		}

		storage.ObjectPinned = true
	}

	for _, b := range req.Detail.Blocks {
		storage, ok := downloadStorageMap[b.StorageID]
		if !ok {
			mod := s.storageMeta.Get(b.StorageID)
			if mod == nil || mod.MasterHub == nil {
				continue
			}

			storage = &downloadStorageInfo{
				Storage:  *mod,
				Distance: s.getStorageDistance(req, *mod),
			}
			downloadStorageMap[b.StorageID] = storage
		}

		storage.Blocks = append(storage.Blocks, b)
	}

	return sort2.Sort(lo.Values(downloadStorageMap), func(left, right *downloadStorageInfo) int {
		return sort2.Cmp(left.Distance, right.Distance)
	})
}

func (s *Selector) getStorageDistance(req request2, src stgmod.StorageDetail) float64 {
	if req.DestHub != nil {
		if src.MasterHub.HubID == req.DestHub.HubID {
			return consts.StorageDistanceSameStorage
		}

		if src.MasterHub.LocationID == req.DestHub.LocationID {
			return consts.StorageDistanceSameLocation
		}

		latency := s.connectivity.Get(src.MasterHub.HubID, req.DestHub.HubID)
		if latency == nil || *latency > time.Duration(float64(time.Millisecond)*s.cfg.HighLatencyHubMs) {
			return consts.HubDistanceHighLatencyHub
		}

		return consts.StorageDistanceOther
	}

	if req.DestLocation != 0 {
		if src.MasterHub.LocationID == req.DestLocation {
			return consts.StorageDistanceSameLocation
		}
	}

	return consts.StorageDistanceOther
}

func (s *Selector) getMinReadingBlockSolution(sortedStgs []*downloadStorageInfo, k int) (float64, []downloadBlock) {
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

func (s *Selector) getMinReadingObjectSolution(sortedStgs []*downloadStorageInfo, k int) (float64, stgmod.StorageDetail) {
	dist := math.MaxFloat64
	var downloadStg stgmod.StorageDetail
	for _, n := range sortedStgs {
		if n.ObjectPinned && float64(k)*n.Distance < dist {
			dist = float64(k) * n.Distance
			stg := n.Storage
			downloadStg = stg
		}
	}

	return dist, downloadStg
}
