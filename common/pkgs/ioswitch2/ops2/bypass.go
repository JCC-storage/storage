package ops2

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/svcmgr"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func init() {
	exec.UseOp[*BypassToShardStore]()
	exec.UseVarValue[*BypassFileInfoValue]()
}

type BypassFileInfoValue struct {
	types.BypassFileInfo
}

func (v *BypassFileInfoValue) Clone() exec.VarValue {
	return &BypassFileInfoValue{
		BypassFileInfo: v.BypassFileInfo,
	}
}

type BypassHandleResultValue struct {
	Commited bool
}

func (r *BypassHandleResultValue) Clone() exec.VarValue {
	return &BypassHandleResultValue{
		Commited: r.Commited,
	}
}

type BypassToShardStore struct {
	StorageID      cdssdk.StorageID
	BypassFileInfo exec.VarID
	BypassCallback exec.VarID
}

func (o *BypassToShardStore) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	svcMgr, err := exec.GetValueByType[*svcmgr.Manager](ctx)
	if err != nil {
		return err
	}

	shardStore, err := svcMgr.GetShardStore(o.StorageID)
	if err != nil {
		return err
	}

	notifier, ok := shardStore.(types.BypassNotifier)
	if !ok {
		return fmt.Errorf("shard store %v not support bypass", o.StorageID)
	}

	fileInfo, err := exec.BindVar[*BypassFileInfoValue](e, ctx.Context, o.BypassFileInfo)
	if err != nil {
		return err
	}

	err = notifier.BypassUploaded(fileInfo.BypassFileInfo)
	if err != nil {
		return err
	}

	e.PutVar(o.BypassCallback, &BypassHandleResultValue{Commited: true})
	return nil
}

func (o *BypassToShardStore) String() string {
	return fmt.Sprintf("BypassToShardStore[StorageID:%v] Info: %v, Callback: %v", o.StorageID, o.BypassFileInfo, o.BypassCallback)
}

type BypassToShardStoreNode struct {
	dag.NodeBase
	StorageID cdssdk.StorageID
}

func (b *GraphNodeBuilder) NewBypassToShardStore(storageID cdssdk.StorageID) *BypassToShardStoreNode {
	node := &BypassToShardStoreNode{
		StorageID: storageID,
	}
	b.AddNode(node)

	node.InputValues().Init(1)
	node.OutputValues().Init(node, 1)
	return node
}

func (n *BypassToShardStoreNode) BypassFileInfoSlot() dag.ValueInputSlot {
	return dag.ValueInputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *BypassToShardStoreNode) BypassCallbackVar() *dag.ValueVar {
	return n.OutputValues().Get(0)
}

func (t *BypassToShardStoreNode) GenerateOp() (exec.Op, error) {
	return &BypassToShardStore{
		StorageID:      t.StorageID,
		BypassFileInfo: t.BypassFileInfoSlot().Var().VarID,
		BypassCallback: t.BypassCallbackVar().VarID,
	}, nil
}
