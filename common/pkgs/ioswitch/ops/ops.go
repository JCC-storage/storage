package ops

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/types"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	"gitlink.org.cn/cloudream/common/utils/serder"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

var OpUnion = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[ioswitch.Op](
	(*IPFSRead)(nil),
	(*IPFSWrite)(nil),
	(*Join)(nil),
)))

type IPFSRead struct {
	Output   ioswitch.StreamID `json:"output"`
	FileHash string            `json:"fileHash"`
}

func (o *IPFSRead) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	logger.
		WithField("FileHash", o.FileHash).
		WithField("Output", o.Output).
		Debugf("ipfs read op")
	defer logger.Debugf("ipfs read op finished")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	file, err := ipfsCli.OpenRead(o.FileHash)
	if err != nil {
		return fmt.Errorf("reading ipfs: %w", err)
	}

	fut := future.NewSetVoid()
	file = myio.AfterReadClosedOnce(file, func(closer io.ReadCloser) {
		fut.SetVoid()
	})

	sw.StreamReady(planID, ioswitch.NewStream(o.Output, file))

	// TODO context
	fut.Wait(context.TODO())
	return nil
}

type IPFSWrite struct {
	Input     ioswitch.StreamID `json:"input"`
	ResultKey string            `json:"resultKey"`
}

func (o *IPFSWrite) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	logger.
		WithField("ResultKey", o.ResultKey).
		WithField("Input", o.Input).
		Debugf("ipfs write op")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	strs, err := sw.WaitStreams(planID, o.Input)
	if err != nil {
		return err
	}
	defer strs[0].Stream.Close()

	fileHash, err := ipfsCli.CreateFile(strs[0].Stream)
	if err != nil {
		return fmt.Errorf("creating ipfs file: %w", err)
	}

	if o.ResultKey != "" {
		sw.AddResultValue(planID, ioswitch.ResultKV{
			Key:   o.ResultKey,
			Value: fileHash,
		})
	}

	return nil
}

type Join struct {
	InputIDs []ioswitch.StreamID `json:"inputIDs"`
	OutputID ioswitch.StreamID   `json:"outputID"`
	Length   int64               `json:"length"`
}

func (o *Join) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	strs, err := sw.WaitStreams(planID, o.InputIDs...)
	if err != nil {
		return err
	}

	var strReaders []io.Reader
	for _, s := range strs {
		strReaders = append(strReaders, s.Stream)
	}
	defer func() {
		for _, str := range strs {
			str.Stream.Close()
		}
	}()

	fut := future.NewSetVoid()
	sw.StreamReady(planID,
		ioswitch.NewStream(o.OutputID,
			myio.AfterReadClosedOnce(myio.Length(myio.Join(strReaders), o.Length), func(closer io.ReadCloser) {
				fut.SetVoid()
			}),
		),
	)

	fut.Wait(context.TODO())
	return nil
}
