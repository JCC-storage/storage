package plans

import (
	"fmt"

	"github.com/google/uuid"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"
)

type StreamInfo struct {
	ID ioswitch.StreamID
}

type PlanBuilder struct {
	streams    []*StreamInfo
	agentPlans map[int64]*AgentPlanBuilder
}

func (b *PlanBuilder) Build() (*ComposedPlan, error) {
	planID := uuid.NewString()

	var agentPlans []AgentPlan
	for _, b := range b.agentPlans {
		plan, err := b.Build(ioswitch.PlanID(planID))
		if err != nil {
			return nil, err
		}

		agentPlans = append(agentPlans, plan)
	}

	return &ComposedPlan{
		ID:         ioswitch.PlanID(planID),
		AgentPlans: agentPlans,
	}, nil
}

func (b *PlanBuilder) newStream() *StreamInfo {
	str := &StreamInfo{
		ID: ioswitch.StreamID(fmt.Sprintf("%d", len(b.streams)+1)),
	}

	b.streams = append(b.streams, str)

	return str
}

func NewPlanBuilder() PlanBuilder {
	return PlanBuilder{
		agentPlans: make(map[int64]*AgentPlanBuilder),
	}
}

func (b *PlanBuilder) FromExecutor() *FromExecutorStream {
	return &FromExecutorStream{
		owner: b,
		info:  b.newStream(),
	}
}

func (b *PlanBuilder) AtAgent(node model.Node) *AgentPlanBuilder {
	agtPlan, ok := b.agentPlans[node.NodeID]
	if !ok {
		agtPlan = &AgentPlanBuilder{
			owner: b,
			node:  node,
		}
		b.agentPlans[node.NodeID] = agtPlan
	}

	return agtPlan
}

type FromExecutorStream struct {
	owner  *PlanBuilder
	info   *StreamInfo
	toNode *model.Node
}

func (s *FromExecutorStream) ToNode(node model.Node) *AgentStream {
	s.toNode = &node
	return &AgentStream{
		owner: s.owner.AtAgent(node),
		info:  s.info,
	}
}

type ToExecutorStream struct {
	info     *StreamInfo
	fromNode *model.Node
}

type AgentStream struct {
	owner *AgentPlanBuilder
	info  *StreamInfo
}

func (s *AgentStream) IPFSWrite(resultKey string) {
	s.owner.ops = append(s.owner.ops, &ops.IPFSWrite{
		Input:     s.info.ID,
		ResultKey: resultKey,
	})
}

func (b *AgentStream) ChunkSplit(chunkSize int, streamCount int, paddingZeros bool) *MultiStream {
	mstr := &MultiStream{}

	var outputStrIDs []ioswitch.StreamID
	for i := 0; i < streamCount; i++ {
		info := b.owner.owner.newStream()
		mstr.Streams = append(mstr.Streams, &AgentStream{
			owner: b.owner,
			info:  info,
		})
		outputStrIDs = append(outputStrIDs, info.ID)
	}

	b.owner.ops = append(b.owner.ops, &ops.ChunkedSplit{
		InputID:      b.info.ID,
		OutputIDs:    outputStrIDs,
		ChunkSize:    chunkSize,
		StreamCount:  streamCount,
		PaddingZeros: paddingZeros,
	})

	return mstr
}

func (s *AgentStream) ToExecutor() *ToExecutorStream {
	return &ToExecutorStream{
		info:     s.info,
		fromNode: &s.owner.node,
	}
}

type AgentPlanBuilder struct {
	owner *PlanBuilder
	node  model.Node
	ops   []ioswitch.Op
}

func (b *AgentPlanBuilder) IPFSRead(fileHash string) *AgentStream {
	agtStr := &AgentStream{
		owner: b,
		info:  b.owner.newStream(),
	}

	b.ops = append(b.ops, &ops.IPFSRead{
		Output:   agtStr.info.ID,
		FileHash: fileHash,
	})

	return agtStr
}

func (b *AgentPlanBuilder) Join(length int64, streams ...*AgentStream) *AgentStream {
	agtStr := &AgentStream{
		owner: b,
		info:  b.owner.newStream(),
	}

	var inputStrIDs []ioswitch.StreamID
	for _, str := range streams {
		inputStrIDs = append(inputStrIDs, str.info.ID)
	}

	b.ops = append(b.ops, &ops.Join{
		InputIDs: inputStrIDs,
		OutputID: agtStr.info.ID,
		Length:   length,
	})

	return agtStr
}

func (b *AgentPlanBuilder) ChunkJoin(chunkSize int, streams ...*AgentStream) *AgentStream {
	agtStr := &AgentStream{
		owner: b,
		info:  b.owner.newStream(),
	}

	var inputStrIDs []ioswitch.StreamID
	for _, str := range streams {
		inputStrIDs = append(inputStrIDs, str.info.ID)
	}

	b.ops = append(b.ops, &ops.ChunkedJoin{
		InputIDs:  inputStrIDs,
		OutputID:  agtStr.info.ID,
		ChunkSize: chunkSize,
	})

	return agtStr
}

func (b *AgentPlanBuilder) Build(planID ioswitch.PlanID) (AgentPlan, error) {
	plan := ioswitch.Plan{
		ID:  planID,
		Ops: b.ops,
	}

	return AgentPlan{
		Plan: plan,
		Node: b.node,
	}, nil
}

type MultiStream struct {
	Streams []*AgentStream
}

func (m *MultiStream) Count() int {
	return len(m.Streams)
}

func (m *MultiStream) Stream(index int) *AgentStream {
	return m.Streams[index]
}
