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
	Input  exec.VarID `json:"input"`
	Output exec.VarID `json:"output"`
	Offset int64      `json:"offset"`
	Length *int64     `json:"length"`
}

func (o *Range) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	input, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer input.Stream.Close()

	buf := make([]byte, 1024*16)

	// 跳过前Offset个字节
	for o.Offset > 0 {
		rdCnt := math2.Min(o.Offset, int64(len(buf)))
		rd, err := input.Stream.Read(buf[:rdCnt])
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

		e.PutVar(o.Output, &exec.StreamValue{
			Stream: io2.AfterEOF(input.Stream, func(closer io.ReadCloser, err error) {
				fut.SetVoid()
			}),
		})
		return fut.Wait(ctx.Context)
	}

	e.PutVar(o.Output, &exec.StreamValue{
		Stream: io2.AfterEOF(io2.Length(input.Stream, *o.Length), func(closer io.ReadCloser, err error) {
			fut.SetVoid()
		}),
	})
	err = fut.Wait(ctx.Context)
	if err != nil {
		return err
	}

	io2.DropWithBuf(input.Stream, buf)
	return nil
}

func (o *Range) String() string {
	len := ""
	if o.Length != nil {
		len = fmt.Sprintf("%v", *o.Length)
	}
	return fmt.Sprintf("Range(%v+%v) %v -> %v", o.Offset, len, o.Input, o.Output)
}

type RangeNode struct {
	dag.NodeBase
	Range math2.Range
}

func (b *GraphNodeBuilder) NewRange() *RangeNode {
	node := &RangeNode{}
	b.AddNode(node)

	node.InputStreams().Init(1)
	node.OutputStreams().Init(node, 1)
	return node
}

func (t *RangeNode) RangeStream(input *dag.StreamVar, rng math2.Range) *dag.StreamVar {
	input.To(t, 0)
	t.Range = rng
	return t.OutputStreams().Get(0)
}

func (t *RangeNode) GenerateOp() (exec.Op, error) {
	return &Range{
		Input:  t.InputStreams().Get(0).VarID,
		Output: t.OutputStreams().Get(0).VarID,
		Offset: t.Range.Offset,
		Length: t.Range.Length,
	}, nil
}

// func (t *RangeType) String() string {
// 	return fmt.Sprintf("Range[%v+%v]%v%v", t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
// }
