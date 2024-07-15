package plans

import (
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"
)

type AgentPlanBuilder struct {
	blder *PlanBuilder
	node  cdssdk.Node
	ops   []ioswitch.Op
}

func (b *AgentPlanBuilder) IPFSRead(fileHash string, opts ...ipfs.ReadOption) *AgentStreamVar {
	opt := ipfs.ReadOption{
		Offset: 0,
		Length: -1,
	}
	if len(opts) > 0 {
		opt = opts[0]
	}

	str := &AgentStreamVar{
		owner: b,
		v:     b.blder.newStreamVar(),
	}

	b.ops = append(b.ops, &ops.IPFSRead{
		Output:   str.v,
		FileHash: fileHash,
		Option:   opt,
	})

	return str
}
func (b *AgentPlanBuilder) FileRead(filePath string) *AgentStreamVar {
	agtStr := &AgentStreamVar{
		owner: b,
		v:     b.blder.newStreamVar(),
	}

	b.ops = append(b.ops, &ops.FileRead{
		Output:   agtStr.v,
		FilePath: filePath,
	})

	return agtStr
}

func (b *AgentPlanBuilder) ECReconstructAny(ec cdssdk.ECRedundancy, inBlockIndexes []int, outBlockIndexes []int, streams []*AgentStreamVar) []*AgentStreamVar {
	var strs []*AgentStreamVar

	var inputStrVars []*ioswitch.StreamVar
	for _, str := range streams {
		inputStrVars = append(inputStrVars, str.v)
	}

	var outputStrVars []*ioswitch.StreamVar
	for i := 0; i < len(outBlockIndexes); i++ {
		v := b.blder.newStreamVar()
		strs = append(strs, &AgentStreamVar{
			owner: b,
			v:     v,
		})
		outputStrVars = append(outputStrVars, v)
	}

	b.ops = append(b.ops, &ops.ECReconstructAny{
		EC:                 ec,
		Inputs:             inputStrVars,
		Outputs:            outputStrVars,
		InputBlockIndexes:  inBlockIndexes,
		OutputBlockIndexes: outBlockIndexes,
	})

	return strs
}

func (b *AgentPlanBuilder) ECReconstruct(ec cdssdk.ECRedundancy, inBlockIndexes []int, streams []*AgentStreamVar) []*AgentStreamVar {
	var strs []*AgentStreamVar

	var inputStrVars []*ioswitch.StreamVar
	for _, str := range streams {
		inputStrVars = append(inputStrVars, str.v)
	}

	var outputStrVars []*ioswitch.StreamVar
	for i := 0; i < ec.K; i++ {
		v := b.blder.newStreamVar()
		strs = append(strs, &AgentStreamVar{
			owner: b,
			v:     v,
		})
		outputStrVars = append(outputStrVars, v)
	}

	b.ops = append(b.ops, &ops.ECReconstruct{
		EC:                ec,
		Inputs:            inputStrVars,
		Outputs:           outputStrVars,
		InputBlockIndexes: inBlockIndexes,
	})

	return strs
}

// 进行galois矩阵乘法运算，ecof * inputs
func (b *AgentPlanBuilder) ECMultiply(coef [][]byte, inputs []*AgentStreamVar, chunkSize int64) []*AgentStreamVar {
	outs := make([]*AgentStreamVar, len(coef))
	outVars := make([]*ioswitch.StreamVar, len(coef))
	for i := 0; i < len(outs); i++ {
		sv := b.blder.newStreamVar()
		outs[i] = &AgentStreamVar{
			owner: b,
			v:     sv,
		}
		outVars[i] = sv
	}

	ins := make([]*ioswitch.StreamVar, len(inputs))
	for i := 0; i < len(inputs); i++ {
		ins[i] = inputs[i].v
	}

	b.ops = append(b.ops, &ops.ECMultiply{
		Inputs:    ins,
		Outputs:   outVars,
		Coef:      coef,
		ChunkSize: chunkSize,
	})

	return outs
}

func (b *AgentPlanBuilder) Join(length int64, streams []*AgentStreamVar) *AgentStreamVar {
	agtStr := &AgentStreamVar{
		owner: b,
		v:     b.blder.newStreamVar(),
	}

	var inputStrVars []*ioswitch.StreamVar
	for _, str := range streams {
		inputStrVars = append(inputStrVars, str.v)
	}

	b.ops = append(b.ops, &ops.Join{
		Inputs: inputStrVars,
		Output: agtStr.v,
		Length: length,
	})

	return agtStr
}

func (b *AgentPlanBuilder) ChunkedJoin(chunkSize int, streams []*AgentStreamVar) *AgentStreamVar {
	agtStr := &AgentStreamVar{
		owner: b,
		v:     b.blder.newStreamVar(),
	}

	var inputStrVars []*ioswitch.StreamVar
	for _, str := range streams {
		inputStrVars = append(inputStrVars, str.v)
	}

	b.ops = append(b.ops, &ops.ChunkedJoin{
		Inputs:    inputStrVars,
		Output:    agtStr.v,
		ChunkSize: chunkSize,
	})

	return agtStr
}

func (b *AgentPlanBuilder) NewString(str string) *AgentStringVar {
	v := b.blder.newStringVar()
	v.Value = str

	return &AgentStringVar{
		owner: b,
		v:     v,
	}
}

func (b *AgentPlanBuilder) NewSignal() *AgentSignalVar {
	v := b.blder.newSignalVar()

	return &AgentSignalVar{
		owner: b,
		v:     v,
	}
}

// 字节流变量
type AgentStreamVar struct {
	owner *AgentPlanBuilder
	v     *ioswitch.StreamVar
}

func (s *AgentStreamVar) IPFSWrite() *AgentStringVar {
	v := s.owner.blder.newStringVar()

	s.owner.ops = append(s.owner.ops, &ops.IPFSWrite{
		Input:    s.v,
		FileHash: v,
	})

	return &AgentStringVar{
		owner: s.owner,
		v:     v,
	}
}

func (b *AgentStreamVar) FileWrite(filePath string) {
	b.owner.ops = append(b.owner.ops, &ops.FileWrite{
		Input:    b.v,
		FilePath: filePath,
	})
}

func (b *AgentStreamVar) ChunkedSplit(chunkSize int, streamCount int, paddingZeros bool) []*AgentStreamVar {
	var strs []*AgentStreamVar

	var outputStrVars []*ioswitch.StreamVar
	for i := 0; i < streamCount; i++ {
		v := b.owner.blder.newStreamVar()
		strs = append(strs, &AgentStreamVar{
			owner: b.owner,
			v:     v,
		})
		outputStrVars = append(outputStrVars, v)
	}

	b.owner.ops = append(b.owner.ops, &ops.ChunkedSplit{
		Input:        b.v,
		Outputs:      outputStrVars,
		ChunkSize:    chunkSize,
		PaddingZeros: paddingZeros,
	})

	return strs
}

func (s *AgentStreamVar) Length(length int64) *AgentStreamVar {
	agtStr := &AgentStreamVar{
		owner: s.owner,
		v:     s.owner.blder.newStreamVar(),
	}

	s.owner.ops = append(s.owner.ops, &ops.Length{
		Input:  s.v,
		Output: agtStr.v,
		Length: length,
	})

	return agtStr
}

func (s *AgentStreamVar) To(node cdssdk.Node) *AgentStreamVar {
	s.owner.ops = append(s.owner.ops, &ops.SendStream{Stream: s.v, Node: node})
	s.owner = s.owner.blder.AtAgent(node)

	return s
}

func (s *AgentStreamVar) ToExecutor() *ExecutorStreamVar {
	s.owner.blder.executorPlan.ops = append(s.owner.blder.executorPlan.ops, &ops.GetStream{
		Stream: s.v,
		Node:   s.owner.node,
	})

	return &ExecutorStreamVar{
		blder: s.owner.blder,
		v:     s.v,
	}
}

func (s *AgentStreamVar) Clone(cnt int) []*AgentStreamVar {
	var strs []*AgentStreamVar

	var outputStrVars []*ioswitch.StreamVar
	for i := 0; i < cnt; i++ {
		v := s.owner.blder.newStreamVar()
		strs = append(strs, &AgentStreamVar{
			owner: s.owner,
			v:     v,
		})
		outputStrVars = append(outputStrVars, v)
	}

	s.owner.ops = append(s.owner.ops, &ops.CloneStream{
		Input:   s.v,
		Outputs: outputStrVars,
	})

	return strs
}

// 当流产生时发送一个信号
func (v *AgentStreamVar) OnBegin() (*AgentStreamVar, *AgentSignalVar) {
	ns := v.owner.blder.newStreamVar()
	s := v.owner.blder.newSignalVar()

	v.owner.ops = append(v.owner.ops, &ops.OnStreamBegin{
		Raw:    v.v,
		New:    ns,
		Signal: s,
	})
	return &AgentStreamVar{owner: v.owner, v: ns}, &AgentSignalVar{owner: v.owner, v: s}
}

// 当流结束时发送一个信号
func (v *AgentStreamVar) OnEnd() (*AgentStreamVar, *AgentSignalVar) {
	ns := v.owner.blder.newStreamVar()
	s := v.owner.blder.newSignalVar()

	v.owner.ops = append(v.owner.ops, &ops.OnStreamEnd{
		Raw:    v.v,
		New:    ns,
		Signal: s,
	})
	return &AgentStreamVar{owner: v.owner, v: ns}, &AgentSignalVar{owner: v.owner, v: s}
}

// 将此流暂存，直到一个信号产生后才释放（一个新流）
func (v *AgentStreamVar) HoldUntil(wait *AgentSignalVar) *AgentStreamVar {
	nv := v.owner.blder.newStreamVar()
	v.owner.ops = append(v.owner.ops, &ops.HoldUntil{
		Waits: []*ioswitch.SignalVar{wait.v},
		Holds: []ioswitch.Var{v.v},
		Emits: []ioswitch.Var{nv},
	})
	return &AgentStreamVar{owner: v.owner, v: nv}
}

// 字符串变量
type AgentStringVar struct {
	owner *AgentPlanBuilder
	v     *ioswitch.StringVar
}

func (v *AgentStringVar) To(node cdssdk.Node) *AgentStringVar {
	v.owner.ops = append(v.owner.ops, &ops.SendVar{Var: v.v, Node: node})
	v.owner = v.owner.blder.AtAgent(node)

	return v
}

func (v *AgentStringVar) ToExecutor() *ExecutorStringVar {
	v.owner.blder.executorPlan.ops = append(v.owner.blder.executorPlan.ops, &ops.GetVar{
		Var:  v.v,
		Node: v.owner.node,
	})

	return &ExecutorStringVar{
		blder: v.owner.blder,
		v:     v.v,
	}
}

func (v *AgentStringVar) Clone() (*AgentStringVar, *AgentStringVar) {
	c1 := v.owner.blder.newStringVar()
	c2 := v.owner.blder.newStringVar()

	v.owner.ops = append(v.owner.ops, &ops.CloneVar{
		Raw:     v.v,
		Cloneds: []ioswitch.Var{c1, c2},
	})

	return &AgentStringVar{owner: v.owner, v: c1}, &AgentStringVar{owner: v.owner, v: c2}
}

// 返回cnt+1个复制后的变量
func (v *AgentStringVar) CloneN(cnt int) []*AgentStringVar {
	var strs []*AgentStringVar
	var cloned []ioswitch.Var
	for i := 0; i < cnt+1; i++ {
		c := v.owner.blder.newStringVar()
		strs = append(strs, &AgentStringVar{
			owner: v.owner,
			v:     c,
		})
		cloned = append(cloned, c)
	}

	v.owner.ops = append(v.owner.ops, &ops.CloneVar{
		Raw:     v.v,
		Cloneds: cloned,
	})

	return strs
}

// 将此变量暂存，直到一个信号产生后才释放（一个新变量）
func (v *AgentStringVar) HoldUntil(wait *AgentSignalVar) *AgentStringVar {
	nv := v.owner.blder.newStringVar()
	v.owner.ops = append(v.owner.ops, &ops.HoldUntil{
		Waits: []*ioswitch.SignalVar{wait.v},
		Holds: []ioswitch.Var{v.v},
		Emits: []ioswitch.Var{nv},
	})
	return &AgentStringVar{owner: v.owner, v: nv}
}

type AgentIntVar struct {
	owner *AgentPlanBuilder
	v     *ioswitch.IntVar
}

// 信号变量
type AgentSignalVar struct {
	owner *AgentPlanBuilder
	v     *ioswitch.SignalVar
}

func (v *AgentSignalVar) To(node cdssdk.Node) *AgentSignalVar {
	v.owner.ops = append(v.owner.ops, &ops.SendVar{Var: v.v, Node: node})
	v.owner = v.owner.blder.AtAgent(node)

	return v
}

func (v *AgentSignalVar) ToExecutor() *ExecutorSignalVar {
	v.owner.blder.executorPlan.ops = append(v.owner.blder.executorPlan.ops, &ops.GetVar{
		Var:  v.v,
		Node: v.owner.node,
	})

	return &ExecutorSignalVar{
		blder: v.owner.blder,
		v:     v.v,
	}
}

// 当这个信号被产生时，同时产生另外n个信号
func (v *AgentSignalVar) Broadcast(cnt int) []*AgentSignalVar {
	var ss []*AgentSignalVar
	var targets []*ioswitch.SignalVar

	for i := 0; i < cnt; i++ {
		c := v.owner.blder.newSignalVar()
		ss = append(ss, &AgentSignalVar{
			owner: v.owner,
			v:     c,
		})
		targets = append(targets, c)
	}

	v.owner.ops = append(v.owner.ops, &ops.Broadcast{
		Source:  v.v,
		Targets: targets,
	})

	return ss
}
