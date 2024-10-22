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
	Raw     *exec.StreamVar   `json:"raw"`
	Cloneds []*exec.StreamVar `json:"cloneds"`
}

func (o *CloneStream) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	err := e.BindVars(ctx.Context, o.Raw)
	if err != nil {
		return err
	}
	defer o.Raw.Stream.Close()

	cloned := io2.Clone(o.Raw.Stream, len(o.Cloneds))

	sem := semaphore.NewWeighted(int64(len(o.Cloneds)))
	for i, s := range cloned {
		sem.Acquire(ctx.Context, 1)

		o.Cloneds[i].Stream = io2.AfterReadClosedOnce(s, func(closer io.ReadCloser) {
			sem.Release(1)
		})
	}
	exec.PutArrayVars(e, o.Cloneds)

	return sem.Acquire(ctx.Context, int64(len(o.Cloneds)))
}

func (o *CloneStream) String() string {
	return fmt.Sprintf("CloneStream %v -> (%v)", o.Raw.ID, utils.FormatVarIDs(o.Cloneds))
}

type CloneVar struct {
	Raw     exec.Var   `json:"raw"`
	Cloneds []exec.Var `json:"cloneds"`
}

func (o *CloneVar) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	err := e.BindVars(ctx.Context, o.Raw)
	if err != nil {
		return err
	}

	for _, v := range o.Cloneds {
		if err := exec.AssignVar(o.Raw, v); err != nil {
			return fmt.Errorf("clone var: %w", err)
		}
	}
	e.PutVars(o.Cloneds...)

	return nil
}

func (o *CloneVar) String() string {
	return fmt.Sprintf("CloneStream %v -> (%v)", o.Raw.GetID(), utils.FormatVarIDs(o.Cloneds))
}

type CloneStreamType struct {
	dag.NodeBase
}

func (b *GraphNodeBuilder) NewCloneStream() *CloneStreamType {
	node := &CloneStreamType{}
	b.AddNode(node)
	return node
}

func (t *CloneStreamType) SetInput(raw *dag.StreamVar) {
	t.InputStreams().EnsureSize(1)
	raw.Connect(t, 0)
}

func (t *CloneStreamType) NewOutput() *dag.StreamVar {
	output := t.Graph().NewStreamVar()
	t.OutputStreams().SetupNew(t, output)
	return output
}

func (t *CloneStreamType) GenerateOp() (exec.Op, error) {
	return &CloneStream{
		Raw: t.InputStreams().Get(0).Var,
		Cloneds: lo.Map(t.OutputStreams().RawArray(), func(v *dag.StreamVar, idx int) *exec.StreamVar {
			return v.Var
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

func (t *CloneVarType) SetInput(raw *dag.ValueVar) {
	t.InputValues().EnsureSize(1)
	raw.Connect(t, 0)
}

func (t *CloneVarType) NewOutput() *dag.ValueVar {
	output := t.Graph().NewValueVar(t.InputValues().Get(0).Type)
	t.OutputValues().SetupNew(t, output)
	return output
}

func (t *CloneVarType) GenerateOp() (exec.Op, error) {
	return &CloneVar{
		Raw: t.InputValues().Get(0).Var,
		Cloneds: lo.Map(t.OutputValues().RawArray(), func(v *dag.ValueVar, idx int) exec.Var {
			return v.Var
		}),
	}, nil
}

// func (t *CloneVarType) String() string {
// 	return fmt.Sprintf("CloneVar[]%v%v", formatStreamIO(node), formatValueIO(node))
// }
