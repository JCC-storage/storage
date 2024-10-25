package ops2

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/utils/io2"
)

func init() {
	// OpUnion.AddT((*Length)(nil))
}

type Length struct {
	Input  exec.VarID `json:"input"`
	Output exec.VarID `json:"output"`
	Length int64      `json:"length"`
}

func (o *Length) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	str, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer str.Stream.Close()

	fut := future.NewSetVoid()
	e.PutVar(o.Output, &exec.StreamValue{
		Stream: io2.AfterReadClosedOnce(io2.Length(str.Stream, o.Length), func(closer io.ReadCloser) {
			fut.SetVoid()
		}),
	})

	return fut.Wait(ctx.Context)
}

func (o *Length) String() string {
	return fmt.Sprintf("Length(length=%v) %v->%v", o.Length, o.Input, o.Output)
}
