package state

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
)

type IndexedStream struct {
	Stream      *dag.StreamVar
	StreamIndex ioswitch2.StreamIndex
}

type GenerateState struct {
	Ft  ioswitch2.FromTo
	DAG *ops2.GraphNodeBuilder
	// 为了产生所有To所需的数据范围，而需要From打开的范围。
	// 这个范围是基于整个文件的，且上下界都取整到条带大小的整数倍，因此上界是有可能超过文件大小的。
	ToNodes        map[ioswitch2.To]ops2.ToNode
	FromNodes      map[ioswitch2.From]ops2.FromNode
	IndexedStreams []IndexedStream
	StreamRange    math2.Range
	UseEC          bool // 是否使用纠删码
	UseSegment     bool // 是否使用分段
}

func InitGenerateState(ft ioswitch2.FromTo) *GenerateState {
	return &GenerateState{
		Ft:        ft,
		DAG:       ops2.NewGraphNodeBuilder(),
		ToNodes:   make(map[ioswitch2.To]ops2.ToNode),
		FromNodes: make(map[ioswitch2.From]ops2.FromNode),
	}
}
