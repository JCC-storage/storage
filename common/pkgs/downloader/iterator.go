package downloader

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader/strategy"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
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
	OnClosing    func()
	downloader   *Downloader
	reqs         []downloadReqeust2
	currentIndex int
}

func NewDownloadObjectIterator(downloader *Downloader, downloadObjs []downloadReqeust2) *DownloadObjectIterator {
	return &DownloadObjectIterator{
		downloader: downloader,
		reqs:       downloadObjs,
	}
}

func (i *DownloadObjectIterator) MoveNext() (*Downloading, error) {
	if i.currentIndex >= len(i.reqs) {
		return nil, iterator.ErrNoMoreItem
	}

	req := i.reqs[i.currentIndex]
	if req.Detail == nil {
		return &Downloading{
			Object:  nil,
			File:    nil,
			Request: req.Raw,
		}, nil
	}

	destHub := cdssdk.HubID(0)
	if stgglb.Local.HubID != nil {
		destHub = *stgglb.Local.HubID
	}

	strg, err := i.downloader.selector.Select(strategy.Request{
		Detail:       *req.Detail,
		Range:        math2.NewRange(req.Raw.Offset, req.Raw.Length),
		DestHub:      destHub,
		DestLocation: stgglb.Local.LocationID,
	})
	if err != nil {
		return nil, fmt.Errorf("selecting download strategy: %w", err)
	}

	var reader io.ReadCloser
	switch strg := strg.(type) {
	case *strategy.DirectStrategy:
		reader, err = i.downloadDirect(req, *strg)
		if err != nil {
			return nil, fmt.Errorf("downloading object %v: %w", req.Raw.ObjectID, err)
		}

	case *strategy.ECReconstructStrategy:
		reader, err = i.downloadECReconstruct(req, *strg)
		if err != nil {
			return nil, fmt.Errorf("downloading ec object %v: %w", req.Raw.ObjectID, err)
		}

	case *strategy.LRCReconstructStrategy:
		reader, err = i.downloadLRCReconstruct(req, *strg)
		if err != nil {
			return nil, fmt.Errorf("downloading lrc object %v: %w", req.Raw.ObjectID, err)
		}

	default:
		return nil, fmt.Errorf("unsupported strategy type: %v", reflect.TypeOf(strg))
	}

	i.currentIndex++
	return &Downloading{
		Object:  &req.Detail.Object,
		File:    reader,
		Request: req.Raw,
	}, nil
}

func (i *DownloadObjectIterator) Close() {
	if i.OnClosing != nil {
		i.OnClosing()
	}
}

func (i *DownloadObjectIterator) downloadDirect(req downloadReqeust2, strg strategy.DirectStrategy) (io.ReadCloser, error) {
	logger.Debugf("downloading object %v from storage %v", req.Raw.ObjectID, strg.Storage.Storage.String())

	var strHandle *exec.DriverReadStream
	ft := ioswitch2.NewFromTo()

	toExec, handle := ioswitch2.NewToDriver(ioswitch2.RawStream())
	toExec.Range = math2.Range{
		Offset: req.Raw.Offset,
	}
	if req.Raw.Length != -1 {
		len := req.Raw.Length
		toExec.Range.Length = &len
	}

	ft.AddFrom(ioswitch2.NewFromShardstore(req.Detail.Object.FileHash, *strg.Storage.MasterHub, strg.Storage, ioswitch2.RawStream())).AddTo(toExec)
	strHandle = handle

	plans := exec.NewPlanBuilder()
	if err := parser.Parse(ft, plans); err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	exeCtx := exec.NewExecContext()
	exec.SetValueByType(exeCtx, i.downloader.stgAgts)
	exec := plans.Execute(exeCtx)
	go exec.Wait(context.TODO())

	return exec.BeginRead(strHandle)
}

func (i *DownloadObjectIterator) downloadECReconstruct(req downloadReqeust2, strg strategy.ECReconstructStrategy) (io.ReadCloser, error) {
	var logStrs []any = []any{fmt.Sprintf("downloading ec object %v from: ", req.Raw.ObjectID)}
	for i, b := range strg.Blocks {
		if i > 0 {
			logStrs = append(logStrs, ", ")
		}

		logStrs = append(logStrs, fmt.Sprintf("%v@%v", b.Index, strg.Storages[i].Storage.String()))
	}
	logger.Debug(logStrs...)

	downloadBlks := make([]downloadBlock, len(strg.Blocks))
	for i, b := range strg.Blocks {
		downloadBlks[i] = downloadBlock{
			Block:   b,
			Storage: strg.Storages[i],
		}
	}

	pr, pw := io.Pipe()
	go func() {
		readPos := req.Raw.Offset
		totalReadLen := req.Detail.Object.Size - req.Raw.Offset
		if req.Raw.Length >= 0 {
			totalReadLen = math2.Min(req.Raw.Length, totalReadLen)
		}

		firstStripIndex := readPos / strg.Redundancy.StripSize()
		stripIter := NewStripIterator(i.downloader, req.Detail.Object, downloadBlks, strg.Redundancy, firstStripIndex, i.downloader.strips, i.downloader.cfg.ECStripPrefetchCount)
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
			curReadLen := math2.Min(totalReadLen, strg.Redundancy.StripSize()-readRelativePos)

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
