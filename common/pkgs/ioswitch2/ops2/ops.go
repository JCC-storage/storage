package ops2

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan/ops"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
)

type GraphNodeBuilder struct {
	*ops.GraphNodeBuilder
}

func NewGraphNodeBuilder() *GraphNodeBuilder {
	return &GraphNodeBuilder{ops.NewGraphNodeBuilder()}
}

type FromNode interface {
	dag.Node
	GetFrom() ioswitch2.From
	Output() dag.StreamOutputSlot
}

type ToNode interface {
	dag.Node
	GetTo() ioswitch2.To
	Input() dag.StreamInputSlot
	SetInput(input *dag.StreamVar)
}
