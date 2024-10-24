package ops2

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/math2"
)

func init() {
	exec.UseOp[*Range]()
}

type Range struct {
	Input  *exec.StreamVar `json:"input"`
	Output *exec.StreamVar `json:"output"`
	Offset int64           `json:"offset"`
	Length *int64          `json:"length"`
}

func (o *Range) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	err := e.BindVars(ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	buf := make([]byte, 1024*16)

	// 跳过前Offset个字节
	for o.Offset > 0 {
		rdCnt := math2.Min(o.Offset, int64(len(buf)))
		rd, err := o.Input.Stream.Read(buf[:rdCnt])
		if err == io.EOF {
			// 输入流不够长度也不报错，只是产生一个空的流
			break
		}
		if err != nil {
			return err
		}
		o.Offset -= int64(rd)
	}

	fut := future.NewSetVoid()

	if o.Length == nil {
		o.Output.Stream = io2.AfterEOF(o.Input.Stream, func(closer io.ReadCloser, err error) {
			fut.SetVoid()
		})

		e.PutVars(o.Output)
		return fut.Wait(ctx.Context)
	}

	o.Output.Stream = io2.AfterEOF(io2.Length(o.Input.Stream, *o.Length), func(closer io.ReadCloser, err error) {
		fut.SetVoid()
	})

	e.PutVars(o.Output)
	err = fut.Wait(ctx.Context)
	if err != nil {
		return err
	}

	io2.DropWithBuf(o.Input.Stream, buf)
	return nil
}

func (o *Range) String() string {
	return fmt.Sprintf("Range(%v+%v) %v -> %v", o.Offset, o.Length, o.Input.ID, o.Output.ID)
}

type RangeNode struct {
	dag.NodeBase
	Range exec.Range
}

func (b *GraphNodeBuilder) NewRange() *RangeNode {
	node := &RangeNode{}
	b.AddNode(node)
	return node
}

func (t *RangeNode) RangeStream(input *dag.StreamVar, rng exec.Range) *dag.StreamVar {
	t.InputStreams().EnsureSize(1)
	input.Connect(t, 0)
	t.Range = rng
	output := t.Graph().NewStreamVar()
	t.OutputStreams().Setup(t, output, 0)
	return output
}

func (t *RangeNode) GenerateOp() (exec.Op, error) {
	return &Range{
		Input:  t.InputStreams().Get(0).Var,
		Output: t.OutputStreams().Get(0).Var,
		Offset: t.Range.Offset,
		Length: t.Range.Length,
	}, nil
}

// func (t *RangeType) String() string {
// 	return fmt.Sprintf("Range[%v+%v]%v%v", t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
// }
