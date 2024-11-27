package ops2

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
)

// TODO 想办法直接使用ops里的Node，而不是重新实现一遍

type FromDriverNode struct {
	dag.NodeBase
	From   ioswitch2.From
	Handle *exec.DriverWriteStream
}

func (b *GraphNodeBuilder) NewFromDriver(fr ioswitch2.From, handle *exec.DriverWriteStream) *FromDriverNode {
	node := &FromDriverNode{
		From:   fr,
		Handle: handle,
	}
	b.AddNode(node)

	node.OutputStreams().Init(node, 1)
	return node
}

func (f *FromDriverNode) GetFrom() ioswitch2.From {
	return f.From
}

func (t *FromDriverNode) Output() dag.StreamSlot {
	return dag.StreamSlot{
		Var:   t.OutputStreams().Get(0),
		Index: 0,
	}
}

func (t *FromDriverNode) GenerateOp() (exec.Op, error) {
	t.Handle.ID = t.OutputStreams().Get(0).VarID
	return nil, nil
}

type ToDriverNode struct {
	dag.NodeBase
	To     ioswitch2.To
	Handle *exec.DriverReadStream
	Range  exec.Range
}

func (b *GraphNodeBuilder) NewToDriver(to ioswitch2.To, handle *exec.DriverReadStream) *ToDriverNode {
	node := &ToDriverNode{
		To:     to,
		Handle: handle,
	}
	b.AddNode(node)

	node.InputStreams().Init(1)
	return node
}

func (t *ToDriverNode) GetTo() ioswitch2.To {
	return t.To
}

func (t *ToDriverNode) SetInput(v *dag.StreamVar) {
	v.To(t, 0)
}

func (t *ToDriverNode) Input() dag.StreamSlot {
	return dag.StreamSlot{
		Var:   t.InputStreams().Get(0),
		Index: 0,
	}
}

func (t *ToDriverNode) GenerateOp() (exec.Op, error) {
	t.Handle.ID = t.InputStreams().Get(0).VarID
	return nil, nil
}
