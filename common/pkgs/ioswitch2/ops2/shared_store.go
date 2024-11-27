package ops2

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/mgr"
)

func init() {
	exec.UseOp[*SharedLoad]()
}

type SharedLoad struct {
	Input          exec.VarID       `json:"input"`
	StorageID      cdssdk.StorageID `json:"storageID"`
	UserID         cdssdk.UserID    `json:"userID"`
	PackageID      cdssdk.PackageID `json:"packageID"`
	Path           string           `json:"path"`
	FullPathOutput exec.VarID       `json:"fullPathOutput"`
}

func (o *SharedLoad) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	logger.
		WithField("Input", o.Input).
		Debugf("load file to shared store")
	defer logger.Debugf("load file to shared store finished")

	stgMgr, err := exec.GetValueByType[*mgr.Manager](ctx)
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

	fullPath, err := store.WritePackageObject(o.UserID, o.PackageID, o.Path, input.Stream)
	if err != nil {
		return fmt.Errorf("writing file to shard store: %w", err)
	}

	if o.FullPathOutput > 0 {
		e.PutVar(o.FullPathOutput, &exec.StringValue{
			Value: fullPath,
		})
	}
	return nil
}

func (o *SharedLoad) String() string {
	return fmt.Sprintf("SharedLoad %v -> %v:%v/%v/%v", o.Input, o.StorageID, o.UserID, o.PackageID, o.Path)
}

type SharedLoadNode struct {
	dag.NodeBase
	To        ioswitch2.To
	StorageID cdssdk.StorageID
	UserID    cdssdk.UserID
	PackageID cdssdk.PackageID
	Path      string
}

func (b *GraphNodeBuilder) NewSharedLoad(to ioswitch2.To, stgID cdssdk.StorageID, userID cdssdk.UserID, packageID cdssdk.PackageID, path string) *SharedLoadNode {
	node := &SharedLoadNode{
		To:        to,
		StorageID: stgID,
		UserID:    userID,
		PackageID: packageID,
		Path:      path,
	}
	b.AddNode(node)

	node.InputStreams().Init(1)
	node.OutputValues().Init(node, 1)
	return node
}

func (t *SharedLoadNode) GetTo() ioswitch2.To {
	return t.To
}

func (t *SharedLoadNode) SetInput(input *dag.StreamVar) {
	input.To(t, 0)
}

func (t *SharedLoadNode) Input() dag.StreamSlot {
	return dag.StreamSlot{
		Var:   t.InputStreams().Get(0),
		Index: 0,
	}
}

func (t *SharedLoadNode) FullPathVar() *dag.ValueVar {
	return t.OutputValues().Get(0)
}

func (t *SharedLoadNode) GenerateOp() (exec.Op, error) {
	return &SharedLoad{
		Input:          t.InputStreams().Get(0).VarID,
		StorageID:      t.StorageID,
		UserID:         t.UserID,
		PackageID:      t.PackageID,
		Path:           t.Path,
		FullPathOutput: t.OutputValues().Get(0).VarID,
	}, nil
}
