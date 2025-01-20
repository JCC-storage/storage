package ops2

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
)

func init() {
	exec.UseOp[*PublicLoad]()
}

type PublicLoad struct {
	Input      exec.VarID
	StorageID  cdssdk.StorageID
	ObjectPath string
}

func (o *PublicLoad) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	logger.
		WithField("Input", o.Input).
		Debugf("load file to public store")
	defer logger.Debugf("load file to public store finished")

	stgAgts, err := exec.GetValueByType[*agtpool.AgentPool](ctx)
	if err != nil {
		return fmt.Errorf("getting storage manager: %w", err)
	}

	store, err := stgAgts.GetPublicStore(o.StorageID)
	if err != nil {
		return fmt.Errorf("getting public store of storage %v: %w", o.StorageID, err)
	}

	input, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer input.Stream.Close()

	return store.Write(o.ObjectPath, input.Stream)
}

func (o *PublicLoad) String() string {
	return fmt.Sprintf("PublicLoad %v -> %v:%v", o.Input, o.StorageID, o.ObjectPath)
}

type PublicLoadNode struct {
	dag.NodeBase
	To         ioswitch2.To
	Storage    stgmod.StorageDetail
	ObjectPath string
}

func (b *GraphNodeBuilder) NewPublicLoad(to ioswitch2.To, stg stgmod.StorageDetail, objPath string) *PublicLoadNode {
	node := &PublicLoadNode{
		To:         to,
		Storage:    stg,
		ObjectPath: objPath,
	}
	b.AddNode(node)

	node.InputStreams().Init(1)
	return node
}

func (t *PublicLoadNode) GetTo() ioswitch2.To {
	return t.To
}

func (t *PublicLoadNode) SetInput(input *dag.StreamVar) {
	input.To(t, 0)
}

func (t *PublicLoadNode) Input() dag.StreamInputSlot {
	return dag.StreamInputSlot{
		Node:  t,
		Index: 0,
	}
}

func (t *PublicLoadNode) GenerateOp() (exec.Op, error) {
	return &PublicLoad{
		Input:      t.InputStreams().Get(0).VarID,
		StorageID:  t.Storage.Storage.StorageID,
		ObjectPath: t.ObjectPath,
	}, nil
}
