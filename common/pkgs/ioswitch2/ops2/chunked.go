package ops2

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/utils"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"golang.org/x/sync/semaphore"
)

func init() {
	exec.UseOp[*ChunkedSplit]()
	exec.UseOp[*ChunkedJoin]()
}

type ChunkedSplit struct {
	Input        exec.VarID   `json:"input"`
	Outputs      []exec.VarID `json:"outputs"`
	ChunkSize    int          `json:"chunkSize"`
	PaddingZeros bool         `json:"paddingZeros"`
}

func (o *ChunkedSplit) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	input, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer input.Stream.Close()

	outputs := io2.ChunkedSplit(input.Stream, o.ChunkSize, len(o.Outputs), io2.ChunkedSplitOption{
		PaddingZeros: o.PaddingZeros,
	})

	sem := semaphore.NewWeighted(int64(len(outputs)))
	for i := range outputs {
		err = sem.Acquire(ctx.Context, 1)
		if err != nil {
			return err
		}

		e.PutVar(o.Outputs[i], &exec.StreamValue{
			Stream: io2.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
				sem.Release(1)
			}),
		})
	}

	return sem.Acquire(ctx.Context, int64(len(outputs)))
}

func (o *ChunkedSplit) String() string {
	return fmt.Sprintf(
		"ChunkedSplit(chunkSize=%v, paddingZeros=%v), %v -> (%v)",
		o.ChunkSize,
		o.PaddingZeros,
		o.Input,
		utils.FormatVarIDs(o.Outputs),
	)
}

type ChunkedJoin struct {
	Inputs    []exec.VarID `json:"inputs"`
	Output    exec.VarID   `json:"output"`
	ChunkSize int          `json:"chunkSize"`
}

func (o *ChunkedJoin) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	inputs, err := exec.BindArray[*exec.StreamValue](e, ctx.Context, o.Inputs)
	if err != nil {
		return err
	}

	var strReaders []io.Reader
	for _, s := range inputs {
		strReaders = append(strReaders, s.Stream)
	}
	defer func() {
		for _, str := range inputs {
			str.Stream.Close()
		}
	}()

	fut := future.NewSetVoid()
	e.PutVar(o.Output, &exec.StreamValue{
		Stream: io2.AfterReadClosedOnce(io2.BufferedChunkedJoin(strReaders, o.ChunkSize), func(closer io.ReadCloser) {
			fut.SetVoid()
		}),
	})

	return fut.Wait(ctx.Context)
}

func (o *ChunkedJoin) String() string {
	return fmt.Sprintf(
		"ChunkedJoin(chunkSize=%v), (%v) -> %v",
		o.ChunkSize,
		utils.FormatVarIDs(o.Inputs),
		o.Output,
	)
}

type ChunkedSplitNode struct {
	dag.NodeBase
	ChunkSize  int
	SplitCount int
}

func (b *GraphNodeBuilder) NewChunkedSplit(chunkSize int, splitCnt int) *ChunkedSplitNode {
	node := &ChunkedSplitNode{
		ChunkSize: chunkSize,
	}
	b.AddNode(node)

	node.InputStreams().Init(1)
	node.OutputStreams().Init(node, splitCnt)
	return node
}

func (t *ChunkedSplitNode) Split(input *dag.StreamVar) {
	input.To(t, 0)
}

func (t *ChunkedSplitNode) SubStream(idx int) *dag.StreamVar {
	return t.OutputStreams().Get(idx)
}

func (t *ChunkedSplitNode) RemoveAllStream() {
	t.InputStreams().ClearAllInput(t)
	t.OutputStreams().ClearAllOutput(t)
	t.OutputStreams().Slots.Resize(0)
}

func (t *ChunkedSplitNode) GenerateOp() (exec.Op, error) {
	return &ChunkedSplit{
		Input:        t.InputStreams().Get(0).VarID,
		Outputs:      t.OutputStreams().GetVarIDs(),
		ChunkSize:    t.ChunkSize,
		PaddingZeros: true,
	}, nil
}

// func (t *ChunkedSplitNode) String() string {
// 	return fmt.Sprintf("ChunkedSplit[%v]%v%v", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
// }

type ChunkedJoinNode struct {
	dag.NodeBase
	ChunkSize int
}

func (b *GraphNodeBuilder) NewChunkedJoin(chunkSize int) *ChunkedJoinNode {
	node := &ChunkedJoinNode{
		ChunkSize: chunkSize,
	}
	b.AddNode(node)
	node.OutputStreams().Init(node, 1)
	return node
}

func (t *ChunkedJoinNode) AddInput(str *dag.StreamVar) {
	idx := t.InputStreams().EnlargeOne()
	str.To(t, idx)
}

func (t *ChunkedJoinNode) Joined() *dag.StreamVar {
	return t.OutputStreams().Get(0)
}

func (t *ChunkedJoinNode) RemoveAllInputs() {
	t.InputStreams().ClearAllInput(t)
	t.InputStreams().Slots.Resize(0)
}

func (t *ChunkedJoinNode) GenerateOp() (exec.Op, error) {
	return &ChunkedJoin{
		Inputs:    t.InputStreams().GetVarIDs(),
		Output:    t.OutputStreams().Get(0).VarID,
		ChunkSize: t.ChunkSize,
	}, nil
}

// func (t *ChunkedJoinType) String() string {
// 	return fmt.Sprintf("ChunkedJoin[%v]%v%v", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
// }
