package ops2

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/utils"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
)

func init() {
	exec.UseOp[*ECMultiply]()
}

type ECMultiply struct {
	Coef      [][]byte     `json:"coef"`
	Inputs    []exec.VarID `json:"inputs"`
	Outputs   []exec.VarID `json:"outputs"`
	ChunkSize int          `json:"chunkSize"`
}

func (o *ECMultiply) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
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

func (o *ECMultiply) String() string {
	return fmt.Sprintf(
		"ECMultiply(coef=%v) (%v) -> (%v)",
		o.Coef,
		utils.FormatVarIDs(o.Inputs),
		utils.FormatVarIDs(o.Outputs),
	)
}

type ECMultiplyNode struct {
	dag.NodeBase
	EC            cdssdk.ECRedundancy
	InputIndexes  []int
	OutputIndexes []int
}

func (b *GraphNodeBuilder) NewECMultiply(ec cdssdk.ECRedundancy) *ECMultiplyNode {
	node := &ECMultiplyNode{
		EC: ec,
	}
	b.AddNode(node)
	return node
}

func (t *ECMultiplyNode) AddInput(str *dag.StreamVar, dataIndex int) {
	t.InputIndexes = append(t.InputIndexes, dataIndex)
	idx := t.InputStreams().EnlargeOne()
	str.To(t, idx)
}

func (t *ECMultiplyNode) RemoveAllInputs() {
	t.InputStreams().ClearAllInput(t)
	t.InputStreams().Slots.Resize(0)
	t.InputIndexes = nil
}

func (t *ECMultiplyNode) NewOutput(dataIndex int) *dag.StreamVar {
	t.OutputIndexes = append(t.OutputIndexes, dataIndex)
	return t.OutputStreams().AppendNew(t).Var
}

func (t *ECMultiplyNode) GenerateOp() (exec.Op, error) {
	rs, err := ec.NewRs(t.EC.K, t.EC.N)
	if err != nil {
		return nil, err
	}
	coef, err := rs.GenerateMatrix(t.InputIndexes, t.OutputIndexes)
	if err != nil {
		return nil, err
	}

	return &ECMultiply{
		Coef:      coef,
		Inputs:    t.InputStreams().GetVarIDs(),
		Outputs:   t.OutputStreams().GetVarIDs(),
		ChunkSize: t.EC.ChunkSize,
	}, nil
}

// func (t *MultiplyType) String() string {
// 	return fmt.Sprintf("Multiply[]%v%v", formatStreamIO(node), formatValueIO(node))
// }
