package downloader

import (
	"context"
	"io"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/math2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
)

type downloadBlock struct {
	Storage stgmod.StorageDetail
	Block   stgmod.ObjectBlock
}

type Strip struct {
	Data     []byte
	Position int64
}

type StripIterator struct {
	downloader               *Downloader
	object                   cdssdk.Object
	blocks                   []downloadBlock
	red                      cdssdk.ECRedundancy
	curStripIndex            int64
	cache                    *StripCache
	dataChan                 chan dataChanEntry
	downloadingDone          chan any
	downloadingDoneOnce      sync.Once
	inited                   bool
	downloadingStream        io.ReadCloser
	downloadingStripIndex    int64
	downloadingPlanCtxCancel func()
}

type dataChanEntry struct {
	Data     []byte
	Position int64 // 条带在文件中的位置。字节为单位
	Error    error
}

func NewStripIterator(downloader *Downloader, object cdssdk.Object, blocks []downloadBlock, red cdssdk.ECRedundancy, beginStripIndex int64, cache *StripCache, maxPrefetch int) *StripIterator {
	if maxPrefetch <= 0 {
		maxPrefetch = 1
	}

	iter := &StripIterator{
		downloader:      downloader,
		object:          object,
		blocks:          blocks,
		red:             red,
		curStripIndex:   beginStripIndex,
		cache:           cache,
		dataChan:        make(chan dataChanEntry, maxPrefetch-1),
		downloadingDone: make(chan any),
	}

	return iter
}

func (s *StripIterator) MoveNext() (Strip, error) {
	if !s.inited {
		go s.downloading(s.curStripIndex)
		s.inited = true
	}

	// 先尝试获取一下，用于判断本次获取是否发生了等待
	select {
	case entry, ok := <-s.dataChan:
		if !ok || entry.Error == io.EOF {
			return Strip{}, iterator.ErrNoMoreItem
		}

		if entry.Error != nil {
			return Strip{}, entry.Error
		}

		s.curStripIndex++
		return Strip{Data: entry.Data, Position: entry.Position}, nil

	default:
		logger.Debugf("waitting for ec strip %v of object %v", s.curStripIndex, s.object.ObjectID)
	}

	// 发生了等待
	select {
	case entry, ok := <-s.dataChan:
		if !ok || entry.Error == io.EOF {
			return Strip{}, iterator.ErrNoMoreItem
		}

		if entry.Error != nil {
			return Strip{}, entry.Error
		}

		s.curStripIndex++
		return Strip{Data: entry.Data, Position: entry.Position}, nil

	case <-s.downloadingDone:
		return Strip{}, iterator.ErrNoMoreItem
	}
}

func (s *StripIterator) Close() {
	s.downloadingDoneOnce.Do(func() {
		close(s.downloadingDone)
	})
}

func (s *StripIterator) downloading(startStripIndex int64) {
	curStripIndex := startStripIndex
loop:
	for {
		stripBytesPos := curStripIndex * int64(s.red.K) * int64(s.red.ChunkSize)
		if stripBytesPos >= s.object.Size {
			s.sendToDataChan(dataChanEntry{Error: io.EOF})
			break
		}

		stripKey := ECStripKey{
			ObjectID:   s.object.ObjectID,
			StripIndex: curStripIndex,
		}

		item, ok := s.cache.Get(stripKey)
		if ok {
			if item.ObjectFileHash == s.object.FileHash {
				if !s.sendToDataChan(dataChanEntry{Data: item.Data, Position: stripBytesPos}) {
					break loop
				}
				curStripIndex++
				continue

			} else {
				// 如果Object的Hash和Cache的Hash不一致，说明Cache是无效的，需要重新下载
				s.cache.Remove(stripKey)
			}
		}

		dataBuf := make([]byte, int64(s.red.K*s.red.ChunkSize))
		n, err := s.readStrip(curStripIndex, dataBuf)
		if err == io.ErrUnexpectedEOF {
			// dataBuf中的内容可能不足一个条带，但仍然将其完整放入cache中，外部应该自行计算该从这个buffer中读多少数据
			s.cache.Add(stripKey, ObjectECStrip{
				Data:           dataBuf,
				ObjectFileHash: s.object.FileHash,
			})

			s.sendToDataChan(dataChanEntry{Data: dataBuf[:n], Position: stripBytesPos})
			s.sendToDataChan(dataChanEntry{Error: io.EOF})
			break loop
		}
		if err != nil {
			s.sendToDataChan(dataChanEntry{Error: err})
			break loop
		}

		s.cache.Add(stripKey, ObjectECStrip{
			Data:           dataBuf,
			ObjectFileHash: s.object.FileHash,
		})

		if !s.sendToDataChan(dataChanEntry{Data: dataBuf, Position: stripBytesPos}) {
			break loop
		}

		curStripIndex++
	}

	if s.downloadingStream != nil {
		s.downloadingStream.Close()
		s.downloadingPlanCtxCancel()
	}
	close(s.dataChan)
}

func (s *StripIterator) sendToDataChan(entry dataChanEntry) bool {
	select {
	case s.dataChan <- entry:
		return true
	case <-s.downloadingDone:
		return false
	}
}

func (s *StripIterator) readStrip(stripIndex int64, buf []byte) (int, error) {
	// 如果需求的条带不当前正在下载的条带的位置不符合，则需要重新打开下载流
	if s.downloadingStream == nil || s.downloadingStripIndex != stripIndex {
		if s.downloadingStream != nil {
			s.downloadingStream.Close()
			s.downloadingPlanCtxCancel()
		}

		ft := ioswitch2.NewFromTo()
		ft.ECParam = &s.red
		for _, b := range s.blocks {
			stg := b.Storage
			ft.AddFrom(ioswitch2.NewFromShardstore(b.Block.FileHash, *stg.MasterHub, stg.Storage, ioswitch2.ECStream(b.Block.Index)))
		}

		toExec, hd := ioswitch2.NewToDriverWithRange(ioswitch2.RawStream(), math2.Range{
			Offset: stripIndex * s.red.StripSize(),
		})
		ft.AddTo(toExec)

		plans := exec.NewPlanBuilder()
		err := parser.Parse(ft, plans)
		if err != nil {
			return 0, err
		}

		exeCtx := exec.NewExecContext()
		exec.SetValueByType(exeCtx, s.downloader.stgAgts)
		exec := plans.Execute(exeCtx)

		ctx, cancel := context.WithCancel(context.Background())
		go exec.Wait(ctx)

		str, err := exec.BeginRead(hd)
		if err != nil {
			cancel()
			return 0, err
		}

		s.downloadingStream = str
		s.downloadingStripIndex = stripIndex
		s.downloadingPlanCtxCancel = cancel
	}

	n, err := io.ReadFull(s.downloadingStream, buf)
	s.downloadingStripIndex += 1
	return n, err
}
