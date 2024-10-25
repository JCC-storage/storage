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
)

func init() {
	// exec.UseOp[*ECReconstructAny]()
	// exec.UseOp[*ECReconstruct]()
	exec.UseOp[*ECMultiply]()
}

/*
	type ECReconstructAny struct {
		EC                 cdssdk.ECRedundancy `json:"ec"`
		Inputs             []exec.VarID        `json:"inputs"`
		Outputs            []exec.VarID        `json:"outputs"`
		InputBlockIndexes  []int               `json:"inputBlockIndexes"`
		OutputBlockIndexes []int               `json:"outputBlockIndexes"`
	}

	func (o *ECReconstructAny) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
		rs, err := ec.NewStreamRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
		if err != nil {
			return fmt.Errorf("new ec: %w", err)
		}

		err = exec.BindArrayVars(e, ctx.Context, inputs)
		if err != nil {
			return err
		}
		defer func() {
			for _, s := range o.Inputs {
				s.Stream.Close()
			}
		}()

		var inputs []io.Reader
		for _, s := range o.Inputs {
			inputs = append(inputs, s.Stream)
		}

		outputs := rs.ReconstructAny(inputs, o.InputBlockIndexes, o.OutputBlockIndexes)

		sem := semaphore.NewWeighted(int64(len(o.Outputs)))
		for i := range o.Outputs {
			sem.Acquire(ctx.Context, 1)

			o.Outputs[i].Stream = io2.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
				sem.Release(1)
			})
		}
		e.PutVar(o.Outputs)

		return sem.Acquire(ctx.Context, int64(len(o.Outputs)))
	}

	type ECReconstruct struct {
		EC                cdssdk.ECRedundancy `json:"ec"`
		Inputs            []exec.VarID        `json:"inputs"`
		Outputs           []exec.VarID        `json:"outputs"`
		InputBlockIndexes []int               `json:"inputBlockIndexes"`
	}

	func (o *ECReconstruct) Execute(ctx context.Context, e *exec.Executor) error {
		rs, err := ec.NewStreamRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
		if err != nil {
			return fmt.Errorf("new ec: %w", err)
		}

		err = exec.BindArrayVars(e, ctx, o.Inputs)
		if err != nil {
			return err
		}
		defer func() {
			for _, s := range o.Inputs {
				s.Stream.Close()
			}
		}()

		var inputs []io.Reader
		for _, s := range o.Inputs {
			inputs = append(inputs, s.Stream)
		}

		outputs := rs.ReconstructData(inputs, o.InputBlockIndexes)

		sem := semaphore.NewWeighted(int64(len(o.Outputs)))
		for i := range o.Outputs {
			sem.Acquire(ctx, 1)

			o.Outputs[i].Stream = io2.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
				sem.Release(1)
			})
		}
		e.PutVar(o.Outputs)

		return sem.Acquire(ctx, int64(len(o.Outputs)))
	}
*/
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
		outputVars[i].Stream = rd
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

func (t *ECMultiplyNode) AddInput(str *dag.Var, dataIndex int) {
	t.InputIndexes = append(t.InputIndexes, dataIndex)
	idx := t.InputStreams().EnlargeOne()
	str.Connect(t, idx)
}

func (t *ECMultiplyNode) RemoveAllInputs() {
	for i, in := range t.InputStreams().RawArray() {
		in.Disconnect(t, i)
	}
	t.InputStreams().Resize(0)
	t.InputIndexes = nil
}

func (t *ECMultiplyNode) NewOutput(dataIndex int) *dag.Var {
	t.OutputIndexes = append(t.OutputIndexes, dataIndex)
	output := t.Graph().NewVar()
	t.OutputStreams().SetupNew(t, output)
	return output
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
		Inputs:    lo.Map(t.InputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID { return v.VarID }),
		Outputs:   lo.Map(t.OutputStreams().RawArray(), func(v *dag.Var, idx int) exec.VarID { return v.VarID }),
		ChunkSize: t.EC.ChunkSize,
	}, nil
}

// func (t *MultiplyType) String() string {
// 	return fmt.Sprintf("Multiply[]%v%v", formatStreamIO(node), formatValueIO(node))
// }
