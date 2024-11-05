package ops2

import (
	"fmt"
	"io"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/utils"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec/lrc"
)

func init() {
	exec.UseOp[*GalMultiply]()
}

type GalMultiply struct {
	Coef      [][]byte     `json:"coef"`
	Inputs    []exec.VarID `json:"inputs"`
	Outputs   []exec.VarID `json:"outputs"`
	ChunkSize int          `json:"chunkSize"`
}

func (o *GalMultiply) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	inputs, err := exec.BindArray[*exec.StreamValue](e, ctx.Context, o.Inputs)
	if err != nil {
		return err
	}
	defer func() {
		for _, s := range inputs {
			s.Stream.Close()
		}
	}()

	outputWrs := make([]*io.PipeWriter, len(o.Outputs))
	outputVars := make([]*exec.StreamValue, len(o.Outputs))
	for i := range o.Outputs {
		rd, wr := io.Pipe()
		outputVars[i] = &exec.StreamValue{Stream: rd}
		outputWrs[i] = wr
	}

	fut := future.NewSetVoid()
	go func() {
		mul := ec.GaloisMultiplier().BuildGalois()

		inputChunks := make([][]byte, len(o.Inputs))
		for i := range o.Inputs {
			inputChunks[i] = make([]byte, o.ChunkSize)
		}
		outputChunks := make([][]byte, len(o.Outputs))
		for i := range o.Outputs {
			outputChunks[i] = make([]byte, o.ChunkSize)
		}

		for {
			err := sync2.ParallelDo(inputs, func(s *exec.StreamValue, i int) error {
				_, err := io.ReadFull(s.Stream, inputChunks[i])
				return err
			})
			if err == io.EOF {
				fut.SetVoid()
				return
			}
			if err != nil {
				fut.SetError(err)
				return
			}

			err = mul.Multiply(o.Coef, inputChunks, outputChunks)
			if err != nil {
				fut.SetError(err)
				return
			}

			for i := range o.Outputs {
				err := io2.WriteAll(outputWrs[i], outputChunks[i])
				if err != nil {
					fut.SetError(err)
					return
				}
			}
		}
	}()

	exec.PutArray(e, o.Outputs, outputVars)
	err = fut.Wait(ctx.Context)
	if err != nil {
		for _, wr := range outputWrs {
			wr.CloseWithError(err)
		}
		return err
	}

	for _, wr := range outputWrs {
		wr.Close()
	}
	return nil
}

func (o *GalMultiply) String() string {
	return fmt.Sprintf(
		"ECMultiply(coef=%v) (%v) -> (%v)",
		o.Coef,
		utils.FormatVarIDs(o.Inputs),
		utils.FormatVarIDs(o.Outputs),
	)
}

type LRCConstructAnyNode struct {
	dag.NodeBase
	LRC           cdssdk.LRCRedundancy
	InputIndexes  []int
	OutputIndexes []int
}

func (b *GraphNodeBuilder) NewLRCConstructAny(lrc cdssdk.LRCRedundancy) *LRCConstructAnyNode {
	node := &LRCConstructAnyNode{
		LRC: lrc,
	}
	b.AddNode(node)
	return node
}

func (t *LRCConstructAnyNode) AddInput(str *dag.Var, dataIndex int) {
	t.InputIndexes = append(t.InputIndexes, dataIndex)
	idx := t.InputStreams().EnlargeOne()
	str.StreamTo(t, idx)
}

func (t *LRCConstructAnyNode) RemoveAllInputs() {
	for i, in := range t.InputStreams().RawArray() {
		in.StreamNotTo(t, i)
	}
	t.InputStreams().Resize(0)
	t.InputIndexes = nil
}

func (t *LRCConstructAnyNode) NewOutput(dataIndex int) *dag.Var {
	t.OutputIndexes = append(t.OutputIndexes, dataIndex)
	output := t.Graph().NewVar()
	t.OutputStreams().SetupNew(t, output)
	return output
}

func (t *LRCConstructAnyNode) GenerateOp() (exec.Op, error) {
	l, err := lrc.New(t.LRC.N, t.LRC.K, t.LRC.Groups)
	if err != nil {
		return nil, err
	}
	coef, err := l.GenerateMatrix(t.InputIndexes, t.OutputIndexes)
	if err != nil {
		return nil, err
	}

	return &GalMultiply{
		Coef:      coef,
		Inputs:    lo.Map(t.InputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID { return v.VarID }),
		Outputs:   lo.Map(t.OutputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID { return v.VarID }),
		ChunkSize: t.LRC.ChunkSize,
	}, nil
}

// func (t *LRCConstructAnyType) String() string {
// 	return fmt.Sprintf("LRCAny[]%v%v", formatStreamIO(node), formatValueIO(node))
// }

type LRCConstructGroupNode struct {
	dag.NodeBase
	LRC              cdssdk.LRCRedundancy
	TargetBlockIndex int
}

func (b *GraphNodeBuilder) NewLRCConstructGroup(lrc cdssdk.LRCRedundancy) *LRCConstructGroupNode {
	node := &LRCConstructGroupNode{
		LRC: lrc,
	}
	b.AddNode(node)
	return node
}

func (t *LRCConstructGroupNode) SetupForTarget(blockIdx int, inputs []*dag.Var) *dag.Var {
	t.TargetBlockIndex = blockIdx

	t.InputStreams().Resize(0)
	for _, in := range inputs {
		idx := t.InputStreams().EnlargeOne()
		in.StreamTo(t, idx)
	}

	output := t.Graph().NewVar()
	t.OutputStreams().Setup(t, output, 0)
	return output
}

func (t *LRCConstructGroupNode) GenerateOp() (exec.Op, error) {
	l, err := lrc.New(t.LRC.N, t.LRC.K, t.LRC.Groups)
	if err != nil {
		return nil, err
	}
	coef, err := l.GenerateGroupMatrix(t.TargetBlockIndex)
	if err != nil {
		return nil, err
	}

	return &GalMultiply{
		Coef:      coef,
		Inputs:    lo.Map(t.InputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID { return v.VarID }),
		Outputs:   lo.Map(t.OutputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID { return v.VarID }),
		ChunkSize: t.LRC.ChunkSize,
	}, nil
}

// func (t *LRCConstructGroupType) String() string {
// 	return fmt.Sprintf("LRCGroup[]%v%v", formatStreamIO(node), formatValueIO(node))
// }
