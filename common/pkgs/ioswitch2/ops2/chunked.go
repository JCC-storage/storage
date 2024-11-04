package ops2

import (
	"fmt"
	"io"

	"github.com/samber/lo"
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
		sem.Acquire(ctx.Context, 1)

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
	ChunkSize int
}

func (b *GraphNodeBuilder) NewChunkedSplit(chunkSize int) *ChunkedSplitNode {
	node := &ChunkedSplitNode{
		ChunkSize: chunkSize,
	}
	b.AddNode(node)
	return node
}

func (t *ChunkedSplitNode) Split(input *dag.Var, cnt int) {
	t.InputStreams().EnsureSize(1)
	input.StreamTo(t, 0)
	t.OutputStreams().Resize(cnt)
	for i := 0; i < cnt; i++ {
		t.OutputStreams().Setup(t, t.Graph().NewVar(), i)
	}
}

func (t *ChunkedSplitNode) SubStream(idx int) *dag.Var {
	return t.OutputStreams().Get(idx)
}

func (t *ChunkedSplitNode) SplitCount() int {
	return t.OutputStreams().Len()
}

func (t *ChunkedSplitNode) Clear() {
	if t.InputStreams().Len() == 0 {
		return
	}

	t.InputStreams().Get(0).StreamNotTo(t, 0)
	t.InputStreams().Resize(0)

	for _, out := range t.OutputStreams().RawArray() {
		out.NoInputAllStream()
	}
	t.OutputStreams().Resize(0)
}

func (t *ChunkedSplitNode) GenerateOp() (exec.Op, error) {
	return &ChunkedSplit{
		Input: t.InputStreams().Get(0).VarID,
		Outputs: lo.Map(t.OutputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID {
			return v.VarID
		}),
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
	node.OutputStreams().SetupNew(node, b.Graph.NewVar())
	return node
}

func (t *ChunkedJoinNode) AddInput(str *dag.Var) {
	idx := t.InputStreams().EnlargeOne()
	str.StreamTo(t, idx)
}

func (t *ChunkedJoinNode) Joined() *dag.Var {
	return t.OutputStreams().Get(0)
}

func (t *ChunkedJoinNode) RemoveAllInputs() {
	for i, in := range t.InputStreams().RawArray() {
		in.StreamNotTo(t, i)
	}
	t.InputStreams().Resize(0)
}

func (t *ChunkedJoinNode) GenerateOp() (exec.Op, error) {
	return &ChunkedJoin{
		Inputs: lo.Map(t.InputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID {
			return v.VarID
		}),
		Output:    t.OutputStreams().Get(0).VarID,
		ChunkSize: t.ChunkSize,
	}, nil
}

// func (t *ChunkedJoinType) String() string {
// 	return fmt.Sprintf("ChunkedJoin[%v]%v%v", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
// }
