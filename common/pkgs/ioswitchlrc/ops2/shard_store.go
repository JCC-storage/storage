package ops2

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/mgr"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func init() {
	exec.UseOp[*ShardRead]()
	exec.UseOp[*ShardWrite]()
	exec.UseVarValue[*FileHashValue]()
}

type FileHashValue struct {
	Hash cdssdk.FileHash `json:"hash"`
}

func (v *FileHashValue) Clone() exec.VarValue {
	return &FileHashValue{Hash: v.Hash}
}

type ShardRead struct {
	Output    exec.VarID       `json:"output"`
	StorageID cdssdk.StorageID `json:"storageID"`
	Open      types.OpenOption `json:"option"`
}

func (o *ShardRead) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	logger.
		WithField("Open", o.Open.String()).
		Debugf("reading from shard store")
	defer logger.Debugf("reading from shard store finished")

	stgMgr, err := exec.GetValueByType[*mgr.Manager](ctx)
	if err != nil {
		return fmt.Errorf("getting storage manager: %w", err)
	}

	store, err := stgMgr.GetShardStore(o.StorageID)
	if err != nil {
		return fmt.Errorf("getting shard store of storage %v: %w", o.StorageID, err)
	}

	file, err := store.Open(o.Open)
	if err != nil {
		return fmt.Errorf("opening shard store file: %w", err)
	}

	fut := future.NewSetVoid()
	e.PutVar(o.Output, &exec.StreamValue{
		Stream: io2.AfterReadClosedOnce(file, func(closer io.ReadCloser) {
			fut.SetVoid()
		}),
	})

	return fut.Wait(ctx.Context)
}

func (o *ShardRead) String() string {
	return fmt.Sprintf("ShardRead %v -> %v", o.Open, o.Output)
}

type ShardWrite struct {
	Input     exec.VarID       `json:"input"`
	FileHash  exec.VarID       `json:"fileHash"`
	StorageID cdssdk.StorageID `json:"storageID"`
}

func (o *ShardWrite) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	logger.
		WithField("Input", o.Input).
		WithField("FileHash", o.FileHash).
		Debugf("writting file to shard store")
	defer logger.Debugf("write to shard store finished")

	stgMgr, err := exec.GetValueByType[*mgr.Manager](ctx)
	if err != nil {
		return fmt.Errorf("getting storage manager: %w", err)
	}

	store, err := stgMgr.GetShardStore(o.StorageID)
	if err != nil {
		return fmt.Errorf("getting shard store of storage %v: %w", o.StorageID, err)
	}

	input, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer input.Stream.Close()

	fileInfo, err := store.Create(input.Stream)
	if err != nil {
		return fmt.Errorf("finishing writing file to shard store: %w", err)
	}

	e.PutVar(o.FileHash, &FileHashValue{
		Hash: fileInfo.Hash,
	})
	return nil
}

func (o *ShardWrite) String() string {
	return fmt.Sprintf("ShardWrite %v -> %v", o.Input, o.FileHash)
}

type ShardReadNode struct {
	dag.NodeBase
	StorageID cdssdk.StorageID
	Open      types.OpenOption
}

func (b *GraphNodeBuilder) NewShardRead(stgID cdssdk.StorageID, open types.OpenOption) *ShardReadNode {
	node := &ShardReadNode{
		StorageID: stgID,
		Open:      open,
	}
	b.AddNode(node)
	node.OutputStreams().SetupNew(node, b.NewVar())
	return node
}

func (t *ShardReadNode) Output() dag.Slot {
	return dag.Slot{
		Var:   t.OutputStreams().Get(0),
		Index: 0,
	}
}

func (t *ShardReadNode) GenerateOp() (exec.Op, error) {
	return &ShardRead{
		Output:    t.OutputStreams().Get(0).VarID,
		StorageID: t.StorageID,
		Open:      t.Open,
	}, nil
}

// func (t *IPFSReadType) String() string {
// 	return fmt.Sprintf("IPFSRead[%s,%v+%v]%v%v", t.FileHash, t.Option.Offset, t.Option.Length, formatStreamIO(node), formatValueIO(node))
// }

type ShardWriteNode struct {
	dag.NodeBase
	FileHashStoreKey string
}

func (b *GraphNodeBuilder) NewShardWrite(fileHashStoreKey string) *ShardWriteNode {
	node := &ShardWriteNode{
		FileHashStoreKey: fileHashStoreKey,
	}
	b.AddNode(node)
	return node
}

func (t *ShardWriteNode) SetInput(input *dag.Var) {
	t.InputStreams().EnsureSize(1)
	input.StreamTo(t, 0)
	t.OutputValues().SetupNew(t, t.Graph().NewVar())
}

func (t *ShardWriteNode) Input() dag.Slot {
	return dag.Slot{
		Var:   t.InputStreams().Get(0),
		Index: 0,
	}
}

func (t *ShardWriteNode) FileHashVar() *dag.Var {
	return t.OutputValues().Get(0)
}

func (t *ShardWriteNode) GenerateOp() (exec.Op, error) {
	return &ShardWrite{
		Input:    t.InputStreams().Get(0).VarID,
		FileHash: t.OutputValues().Get(0).VarID,
	}, nil
}

// func (t *IPFSWriteType) String() string {
// 	return fmt.Sprintf("IPFSWrite[%s,%v+%v]%v%v", t.FileHashStoreKey, t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
// }
