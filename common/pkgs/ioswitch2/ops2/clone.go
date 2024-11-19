package ops2

import (
	"fmt"
	"io"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/utils"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"golang.org/x/sync/semaphore"
)

func init() {
	exec.UseOp[*CloneStream]()
	exec.UseOp[*CloneVar]()
}

type CloneStream struct {
	Raw     exec.VarID   `json:"raw"`
	Cloneds []exec.VarID `json:"cloneds"`
}

func (o *CloneStream) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	raw, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Raw)
	if err != nil {
		return err
	}
	defer raw.Stream.Close()

	cloned := io2.Clone(raw.Stream, len(o.Cloneds))

	sem := semaphore.NewWeighted(int64(len(o.Cloneds)))
	for i, s := range cloned {
		err = sem.Acquire(ctx.Context, 1)
		if err != nil {
			return err
		}

		e.PutVar(o.Cloneds[i], &exec.StreamValue{
			Stream: io2.AfterReadClosedOnce(s, func(closer io.ReadCloser) {
				sem.Release(1)
			}),
		})
	}

	return sem.Acquire(ctx.Context, int64(len(o.Cloneds)))
}

func (o *CloneStream) String() string {
	return fmt.Sprintf("CloneStream %v -> (%v)", o.Raw, utils.FormatVarIDs(o.Cloneds))
}

type CloneVar struct {
	Raw     exec.VarID   `json:"raw"`
	Cloneds []exec.VarID `json:"cloneds"`
}

func (o *CloneVar) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	raw, err := e.BindVar(ctx.Context, o.Raw)
	if err != nil {
		return err
	}

	for i := range o.Cloneds {
		e.PutVar(o.Cloneds[i], raw.Clone())
	}

	return nil
}

func (o *CloneVar) String() string {
	return fmt.Sprintf("CloneStream %v -> (%v)", o.Raw, utils.FormatVarIDs(o.Cloneds))
}

type CloneStreamType struct {
	dag.NodeBase
}

func (b *GraphNodeBuilder) NewCloneStream() *CloneStreamType {
	node := &CloneStreamType{}
	b.AddNode(node)
	return node
}

func (t *CloneStreamType) SetInput(raw *dag.Var) {
	t.InputStreams().EnsureSize(1)
	raw.StreamTo(t, 0)
}

func (t *CloneStreamType) NewOutput() *dag.Var {
	output := t.Graph().NewVar()
	t.OutputStreams().SetupNew(t, output)
	return output
}

func (t *CloneStreamType) GenerateOp() (exec.Op, error) {
	return &CloneStream{
		Raw: t.InputStreams().Get(0).VarID,
		Cloneds: lo.Map(t.OutputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID {
			return v.VarID
		}),
	}, nil
}

// func (t *CloneStreamType) String() string {
// 	return fmt.Sprintf("CloneStream[]%v%v", formatStreamIO(node), formatValueIO(node))
// }

type CloneVarType struct {
	dag.NodeBase
}

func (b *GraphNodeBuilder) NewCloneValue() *CloneVarType {
	node := &CloneVarType{}
	b.AddNode(node)
	return node
}

func (t *CloneVarType) SetInput(raw *dag.Var) {
	t.InputValues().EnsureSize(1)
	raw.ValueTo(t, 0)
}

func (t *CloneVarType) NewOutput() *dag.Var {
	output := t.Graph().NewVar()
	t.OutputValues().SetupNew(t, output)
	return output
}

func (t *CloneVarType) GenerateOp() (exec.Op, error) {
	return &CloneVar{
		Raw: t.InputValues().Get(0).VarID,
		Cloneds: lo.Map(t.OutputValues().RawArray(), func(v *dag.Var, idx int) exec.VarID {
			return v.VarID
		}),
	}, nil
}

// func (t *CloneVarType) String() string {
// 	return fmt.Sprintf("CloneVar[]%v%v", formatStreamIO(node), formatValueIO(node))
// }
