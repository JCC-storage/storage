package ops2

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/svcmgr"
)

func init() {
	exec.UseOp[*SharedLoad]()
}

type SharedLoad struct {
	Input      exec.VarID
	StorageID  cdssdk.StorageID
	ObjectPath string
}

func (o *SharedLoad) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	logger.
		WithField("Input", o.Input).
		Debugf("load file to shared store")
	defer logger.Debugf("load file to shared store finished")

	stgMgr, err := exec.GetValueByType[*svcmgr.AgentPool](ctx)
	if err != nil {
		return fmt.Errorf("getting storage manager: %w", err)
	}

	store, err := stgMgr.GetSharedStore(o.StorageID)
	if err != nil {
		return fmt.Errorf("getting shard store of storage %v: %w", o.StorageID, err)
	}

	input, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer input.Stream.Close()

	return store.Write(o.ObjectPath, input.Stream)
}

func (o *SharedLoad) String() string {
	return fmt.Sprintf("SharedLoad %v -> %v:%v", o.Input, o.StorageID, o.ObjectPath)
}

type SharedLoadNode struct {
	dag.NodeBase
	To         ioswitch2.To
	StorageID  cdssdk.StorageID
	ObjectPath string
}

func (b *GraphNodeBuilder) NewSharedLoad(to ioswitch2.To, stgID cdssdk.StorageID, objPath string) *SharedLoadNode {
	node := &SharedLoadNode{
		To:         to,
		StorageID:  stgID,
		ObjectPath: objPath,
	}
	b.AddNode(node)

	node.InputStreams().Init(1)
	return node
}

func (t *SharedLoadNode) GetTo() ioswitch2.To {
	return t.To
}

func (t *SharedLoadNode) SetInput(input *dag.StreamVar) {
	input.To(t, 0)
}

func (t *SharedLoadNode) Input() dag.StreamInputSlot {
	return dag.StreamInputSlot{
		Node:  t,
		Index: 0,
	}
}

func (t *SharedLoadNode) GenerateOp() (exec.Op, error) {
	return &SharedLoad{
		Input:      t.InputStreams().Get(0).VarID,
		StorageID:  t.StorageID,
		ObjectPath: t.ObjectPath,
	}, nil
}
