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
	"gitlink.org.cn/cloudream/storage/common/pkgs/shardstore/pool"
	"gitlink.org.cn/cloudream/storage/common/pkgs/shardstore/types"
)

func init() {
	exec.UseOp[*ShardRead]()
	exec.UseOp[*ShardWrite]()
}

type ShardRead struct {
	Output    *exec.StreamVar  `json:"output"`
	StorageID cdssdk.StorageID `json:"storageID"`
	Open      types.OpenOption `json:"option"`
}

func (o *ShardRead) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	logger.
		WithField("Open", o.Open).
		Debugf("reading from shard store")
	defer logger.Debugf("reading from shard store finished")

	pool, err := exec.ValueByType[*pool.ShardStorePool](ctx)
	if err != nil {
		return fmt.Errorf("getting shard store pool: %w", err)
	}

	store, err := pool.Get(o.StorageID)
	if err != nil {
		return fmt.Errorf("getting shard store %v: %w", o.StorageID, err)
	}

	file, err := store.Open(o.Open)
	if err != nil {
		return fmt.Errorf("opening shard store file: %w", err)
	}

	fut := future.NewSetVoid()
	o.Output.Stream = io2.AfterReadClosedOnce(file, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	e.PutVars(o.Output)

	return fut.Wait(ctx.Context)
}

func (o *ShardRead) String() string {
	return fmt.Sprintf("ShardRead %v -> %v", o.Open, o.Output.ID)
}

type ShardWrite struct {
	Input     *exec.StreamVar  `json:"input"`
	FileHash  *exec.StringVar  `json:"fileHash"`
	StorageID cdssdk.StorageID `json:"storageID"`
}

func (o *ShardWrite) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	logger.
		WithField("Input", o.Input.ID).
		WithField("FileHashVar", o.FileHash.ID).
		Debugf("writting file to shard store")
	defer logger.Debugf("write to shard store finished")

	pool, err := exec.ValueByType[*pool.ShardStorePool](ctx)
	if err != nil {
		return fmt.Errorf("getting shard store pool: %w", err)
	}

	store, err := pool.Get(o.StorageID)
	if err != nil {
		return fmt.Errorf("getting shard store %v: %w", o.StorageID, err)
	}

	err = e.BindVars(ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	writer := store.New()
	defer writer.Abort()

	_, err = io.Copy(writer, o.Input.Stream)
	if err != nil {
		return fmt.Errorf("writing file to shard store: %w", err)
	}

	fileInfo, err := writer.Finish()
	if err != nil {
		return fmt.Errorf("finishing writing file to shard store: %w", err)
	}

	o.FileHash.Value = string(fileInfo.Hash)

	e.PutVars(o.FileHash)
	return nil
}

func (o *ShardWrite) String() string {
	return fmt.Sprintf("IPFSWrite %v -> %v", o.Input.ID, o.FileHash.ID)
}

type ShardReadNode struct {
	dag.NodeBase
	StorageID cdssdk.StorageID
	Open      types.OpenOption
}

func (b *GraphNodeBuilder) NewIPFSRead(stgID cdssdk.StorageID, open types.OpenOption) *ShardReadNode {
	node := &ShardReadNode{
		StorageID: stgID,
		Open:      open,
	}
	b.AddNode(node)
	node.OutputStreams().SetupNew(node, b.NewStreamVar())
	return node
}

func (t *ShardReadNode) Output() dag.StreamSlot {
	return dag.StreamSlot{
		Var:   t.OutputStreams().Get(0),
		Index: 0,
	}
}

func (t *ShardReadNode) GenerateOp() (exec.Op, error) {
	return &ShardRead{
		Output:    t.OutputStreams().Get(0).Var,
		StorageID: t.StorageID,
		Open:      t.Open,
	}, nil
}

// func (t *IPFSReadType) String() string {
// 	return fmt.Sprintf("IPFSRead[%s,%v+%v]%v%v", t.FileHash, t.Option.Offset, t.Option.Length, formatStreamIO(node), formatValueIO(node))
// }

type IPFSWriteNode struct {
	dag.NodeBase
	FileHashStoreKey string
}

func (b *GraphNodeBuilder) NewIPFSWrite(fileHashStoreKey string) *IPFSWriteNode {
	node := &IPFSWriteNode{
		FileHashStoreKey: fileHashStoreKey,
	}
	b.AddNode(node)
	return node
}

func (t *IPFSWriteNode) SetInput(input *dag.StreamVar) {
	t.InputStreams().EnsureSize(1)
	input.Connect(t, 0)
	t.OutputValues().SetupNew(t, t.Graph().NewValueVar(dag.StringValueVar))
}

func (t *IPFSWriteNode) Input() dag.StreamSlot {
	return dag.StreamSlot{
		Var:   t.InputStreams().Get(0),
		Index: 0,
	}
}

func (t *IPFSWriteNode) FileHashVar() *dag.ValueVar {
	return t.OutputValues().Get(0)
}

func (t *IPFSWriteNode) GenerateOp() (exec.Op, error) {
	return &ShardWrite{
		Input:    t.InputStreams().Get(0).Var,
		FileHash: t.OutputValues().Get(0).Var.(*exec.StringVar),
	}, nil
}

// func (t *IPFSWriteType) String() string {
// 	return fmt.Sprintf("IPFSWrite[%s,%v+%v]%v%v", t.FileHashStoreKey, t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
// }
