package downloader

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader/strategy"
)

func (iter *DownloadObjectIterator) downloadLRCReconstruct(req downloadReqeust2, strg strategy.LRCReconstructStrategy) (io.ReadCloser, error) {
	var logStrs []any = []any{fmt.Sprintf("downloading lrc object %v from: ", req.Raw.ObjectID)}
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

		firstStripIndex := readPos / int64(strg.Redundancy.K) / int64(strg.Redundancy.ChunkSize)
		stripIter := NewLRCStripIterator(iter.downloader, req.Detail.Object, downloadBlks, strg.Redundancy, firstStripIndex, iter.downloader.strips, iter.downloader.cfg.ECStripPrefetchCount)
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
			nextStripPos := strip.Position + int64(strg.Redundancy.K)*int64(strg.Redundancy.ChunkSize)
			curReadLen := math2.Min(totalReadLen, nextStripPos-readPos)

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
