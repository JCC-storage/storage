package ops2

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/io2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
)

func init() {
	exec.UseOp[*IPFSRead]()
	exec.UseOp[*IPFSWrite]()
}

type IPFSRead struct {
	Output   *exec.StreamVar `json:"output"`
	FileHash string          `json:"fileHash"`
	Option   ipfs.ReadOption `json:"option"`
}

func (o *IPFSRead) Execute(ctx context.Context, e *exec.Executor) error {
	logger.
		WithField("FileHash", o.FileHash).
		Debugf("ipfs read op")
	defer logger.Debugf("ipfs read op finished")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	file, err := ipfsCli.OpenRead(o.FileHash, o.Option)
	if err != nil {
		return fmt.Errorf("reading ipfs: %w", err)
	}
	defer file.Close()

	fut := future.NewSetVoid()
	rb := io2.RingBuffer(file, 16*1024)
	rb.UpstreamName = "IPFS"
	rb.DownstreamName = fmt.Sprintf("IPFS output %v", o.Output.ID)
	o.Output.Stream = io2.AfterReadClosedOnce(rb, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	e.PutVars(o.Output)

	return fut.Wait(ctx)
}

func (o *IPFSRead) String() string {
	return fmt.Sprintf("IPFSRead %v -> %v", o.FileHash, o.Output.ID)
}

type IPFSWrite struct {
	Input    *exec.StreamVar `json:"input"`
	FileHash *exec.StringVar `json:"fileHash"`
}

func (o *IPFSWrite) Execute(ctx context.Context, e *exec.Executor) error {
	logger.
		WithField("Input", o.Input.ID).
		WithField("FileHashVar", o.FileHash.ID).
		Debugf("ipfs write op")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	err = e.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	o.FileHash.Value, err = ipfsCli.CreateFile(o.Input.Stream)
	if err != nil {
		return fmt.Errorf("creating ipfs file: %w", err)
	}

	e.PutVars(o.FileHash)

	return nil
}

func (o *IPFSWrite) String() string {
	return fmt.Sprintf("IPFSWrite %v -> %v", o.Input.ID, o.FileHash.ID)
}

type IPFSReadNode struct {
	dag.NodeBase
	FileHash string
	Option   ipfs.ReadOption
}

func (b *GraphNodeBuilder) NewIPFSRead(fileHash string, option ipfs.ReadOption) *IPFSReadNode {
	node := &IPFSReadNode{
		FileHash: fileHash,
		Option:   option,
	}
	b.AddNode(node)
	node.OutputStreams().SetupNew(node, b.NewStreamVar())
	return node
}

func (t *IPFSReadNode) Output() dag.StreamSlot {
	return dag.StreamSlot{
		Var:   t.OutputStreams().Get(0),
		Index: 0,
	}
}

func (t *IPFSReadNode) GenerateOp() (exec.Op, error) {
	return &IPFSRead{
		Output:   t.OutputStreams().Get(0).Var,
		FileHash: t.FileHash,
		Option:   t.Option,
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
	return &IPFSWrite{
		Input:    t.InputStreams().Get(0).Var,
		FileHash: t.OutputValues().Get(0).Var.(*exec.StringVar),
	}, nil
}

// func (t *IPFSWriteType) String() string {
// 	return fmt.Sprintf("IPFSWrite[%s,%v+%v]%v%v", t.FileHashStoreKey, t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
// }
