package ops

import (
	"io"
	"sync"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type ChunkedSplit struct {
	InputID      ioswitch.StreamID   `json:"inputID"`
	OutputIDs    []ioswitch.StreamID `json:"outputIDs"`
	ChunkSize    int                 `json:"chunkSize"`
	StreamCount  int                 `json:"streamCount"`
	PaddingZeros bool                `json:"paddingZeros"`
}

func (o *ChunkedSplit) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	str, err := sw.WaitStreams(planID, o.InputID)
	if err != nil {
		return err
	}
	defer str[0].Stream.Close()

	wg := sync.WaitGroup{}
	outputs := myio.ChunkedSplit(str[0].Stream, o.ChunkSize, o.StreamCount, myio.ChunkedSplitOption{
		PaddingZeros: o.PaddingZeros,
	})

	for i := range outputs {
		wg.Add(1)

		sw.StreamReady(planID, ioswitch.NewStream(
			o.OutputIDs[i],
			myio.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
				wg.Done()
			}),
		))
	}

	wg.Wait()

	return nil
}

func init() {
	OpUnion.AddT((*ChunkedSplit)(nil))
}