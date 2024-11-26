package ops2

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/utils"
	"gitlink.org.cn/cloudream/common/utils/io2"
)

func init() {
	exec.UseOp[*SegmentSplit]()
	exec.UseOp[*SegmentJoin]()
}

type SegmentSplit struct {
	Input    exec.VarID
	Segments []int64
	Outputs  []exec.VarID
}

func (o *SegmentSplit) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	input, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer input.Stream.Close()

	for i, outID := range o.Outputs {
		fut := future.NewSetVoid()

		segStr := io.LimitReader(input.Stream, o.Segments[i])
		segStr2 := io2.DelegateReadCloser(segStr, func() error {
			fut.SetError(context.Canceled)
			return nil
		})

		segStr2 = io2.AfterEOF(segStr2, func(str io.ReadCloser, err error) {
			fut.SetVoid()
		})

		e.PutVar(outID, &exec.StreamValue{Stream: segStr2})
		err = fut.Wait(ctx.Context)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *SegmentSplit) String() string {
	return fmt.Sprintf("SegmentSplit(%v, %v) -> %v", o.Input, o.Segments, o.Outputs)
}

type SegmentJoin struct {
	Inputs []exec.VarID
	Output exec.VarID
	// 这些字段只在执行时使用
	ctx           *exec.ExecContext
	e             *exec.Executor
	nextStreamIdx int
	nextStream    io.ReadCloser
	fut           *future.SetVoidFuture
}

func (o *SegmentJoin) Read(buf []byte) (int, error) {
	for {
		if o.nextStream == nil {
			if o.nextStreamIdx >= len(o.Inputs) {
				o.fut.SetVoid()
				return 0, io.EOF
			}

			input, err := exec.BindVar[*exec.StreamValue](o.e, o.ctx.Context, o.Inputs[o.nextStreamIdx])
			if err != nil {
				return 0, err
			}

			o.nextStream = input.Stream
			o.nextStreamIdx++
		}

		n, err := o.nextStream.Read(buf)
		if err == io.EOF {
			o.nextStream.Close()
			o.nextStream = nil
			continue
		}
		return n, err
	}
}

func (o *SegmentJoin) Close() error {
	if o.nextStream != nil {
		o.nextStream.Close()
		o.nextStream = nil
		o.fut.SetVoid()
	}

	return nil
}

func (o *SegmentJoin) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	o.ctx = ctx
	o.e = e
	o.nextStreamIdx = 0
	o.nextStream = nil
	o.fut = future.NewSetVoid()

	e.PutVar(o.Output, &exec.StreamValue{Stream: o})
	return o.fut.Wait(ctx.Context)
}

func (o *SegmentJoin) String() string {
	return fmt.Sprintf("SegmentJoin %v -> %v", utils.FormatVarIDs(o.Inputs), o.Output)
}

type SegmentSplitNode struct {
	dag.NodeBase
	segments []int64
}

func (b *GraphNodeBuilder) NewSegmentSplit(segments []int64) *SegmentSplitNode {
	node := &SegmentSplitNode{
		segments: segments,
	}
	b.AddNode(node)
	node.OutputStreams().Resize(len(segments))
	return node
}

func (n *SegmentSplitNode) SetInput(input *dag.Var) {
	n.InputStreams().EnsureSize(1)
	input.StreamTo(n, 0)
}

func (n *SegmentSplitNode) Segment(index int) *dag.Var {
	// 必须连续消耗流
	for i := 0; i <= index; i++ {
		if n.OutputStreams().Get(i) == nil {
			n.OutputStreams().Setup(n, n.Graph().NewVar(), i)
		}
	}
	return n.OutputStreams().Get(index)
}

func (t *SegmentSplitNode) GenerateOp() (exec.Op, error) {
	lastUsedSeg := 0
	for i := t.OutputStreams().Len() - 1; i >= 0; i-- {
		if t.OutputStreams().Get(i) != nil {
			lastUsedSeg = i
			break
		}
	}

	return &SegmentSplit{
		Input:    t.InputStreams().Get(0).VarID,
		Segments: t.segments[:lastUsedSeg+1],
		Outputs:  t.OutputStreams().GetVarIDs(),
	}, nil
}

type SegmentJoinNode struct {
	dag.NodeBase
	UsedStart int
	UsedCount int
}

func (b *GraphNodeBuilder) NewSegmentJoin(segmentSizes []int64) *SegmentJoinNode {
	node := &SegmentJoinNode{}
	b.AddNode(node)
	node.InputStreams().Resize(len(segmentSizes))
	node.OutputStreams().SetupNew(node, b.NewVar())
	return node
}

func (n *SegmentJoinNode) SetInput(index int, input *dag.Var) {
	input.StreamTo(n, index)
}

// 记录本计划中实际要使用的分段的范围，范围外的分段流都会取消输入
func (n *SegmentJoinNode) MarkUsed(start, cnt int) {
	n.UsedStart = start
	n.UsedCount = cnt

	for i := 0; i < start; i++ {
		str := n.InputStreams().Get(i)
		if str != nil {
			str.StreamNotTo(n, i)
		}
	}

	for i := start + cnt; i < n.InputStreams().Len(); i++ {
		str := n.InputStreams().Get(i)
		if str != nil {
			str.StreamNotTo(n, i)
		}
	}
}

func (n *SegmentJoinNode) Joined() *dag.Var {
	return n.OutputStreams().Get(0)
}

func (t *SegmentJoinNode) GenerateOp() (exec.Op, error) {
	return &SegmentJoin{
		Inputs: t.InputStreams().GetVarIDsRanged(t.UsedStart, t.UsedStart+t.UsedCount),
		Output: t.OutputStreams().Get(0).VarID,
	}, nil
}
