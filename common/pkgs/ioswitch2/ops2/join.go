package ops2

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/utils"
	"gitlink.org.cn/cloudream/common/utils/io2"
)

func init() {
	exec.UseOp[*Join]()
}

type Join struct {
	Inputs []exec.VarID `json:"inputs"`
	Output exec.VarID   `json:"output"`
	Length int64        `json:"length"`
}

func (o *Join) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
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
		Stream: io2.AfterReadClosedOnce(io2.Length(io2.Join(strReaders), o.Length), func(closer io.ReadCloser) {
			fut.SetVoid()
		}),
	})

	return fut.Wait(ctx.Context)
}

func (o *Join) String() string {
	return fmt.Sprintf("Join %v->%v", utils.FormatVarIDs(o.Inputs), o.Output)
}
