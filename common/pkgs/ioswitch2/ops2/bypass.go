package ops2

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func init() {
	exec.UseOp[*BypassToShardStore]()
	exec.UseVarValue[*BypassUploadedFileValue]()
	exec.UseVarValue[*BypassHandleResultValue]()

	exec.UseOp[*BypassFromShardStore]()
	exec.UseVarValue[*BypassFilePathValue]()

	exec.UseOp[*BypassFromShardStoreHTTP]()
	exec.UseVarValue[*HTTPRequestValue]()
}

type BypassUploadedFileValue struct {
	types.BypassUploadedFile
}

func (v *BypassUploadedFileValue) Clone() exec.VarValue {
	return &BypassUploadedFileValue{
		BypassUploadedFile: v.BypassUploadedFile,
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
	FileHash       exec.VarID
}

func (o *BypassToShardStore) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	stgAgts, err := exec.GetValueByType[*agtpool.AgentPool](ctx)
	if err != nil {
		return err
	}

	shardStore, err := stgAgts.GetShardStore(o.StorageID)
	if err != nil {
		return err
	}

	br, ok := shardStore.(types.BypassWrite)
	if !ok {
		return fmt.Errorf("shard store %v not support bypass write", o.StorageID)
	}

	fileInfo, err := exec.BindVar[*BypassUploadedFileValue](e, ctx.Context, o.BypassFileInfo)
	if err != nil {
		return err
	}

	err = br.BypassUploaded(fileInfo.BypassUploadedFile)
	if err != nil {
		return err
	}

	e.PutVar(o.BypassCallback, &BypassHandleResultValue{Commited: true})
	e.PutVar(o.FileHash, &FileHashValue{Hash: fileInfo.Hash})
	return nil
}

func (o *BypassToShardStore) String() string {
	return fmt.Sprintf("BypassToShardStore[StorageID:%v] Info: %v, Callback: %v", o.StorageID, o.BypassFileInfo, o.BypassCallback)
}

type BypassFilePathValue struct {
	types.BypassFilePath
}

func (v *BypassFilePathValue) Clone() exec.VarValue {
	return &BypassFilePathValue{
		BypassFilePath: v.BypassFilePath,
	}
}

type BypassFromShardStore struct {
	StorageID cdssdk.StorageID
	FileHash  cdssdk.FileHash
	Output    exec.VarID
}

func (o *BypassFromShardStore) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	stgAgts, err := exec.GetValueByType[*agtpool.AgentPool](ctx)
	if err != nil {
		return err
	}

	shardStore, err := stgAgts.GetShardStore(o.StorageID)
	if err != nil {
		return err
	}

	br, ok := shardStore.(types.BypassRead)
	if !ok {
		return fmt.Errorf("shard store %v not support bypass read", o.StorageID)
	}

	path, err := br.BypassRead(o.FileHash)
	if err != nil {
		return err
	}

	e.PutVar(o.Output, &BypassFilePathValue{BypassFilePath: path})
	return nil
}

func (o *BypassFromShardStore) String() string {
	return fmt.Sprintf("BypassFromShardStore[StorageID:%v] FileHash: %v, Output: %v", o.StorageID, o.FileHash, o.Output)
}

// 旁路Http读取
type BypassFromShardStoreHTTP struct {
	StorageID cdssdk.StorageID
	FileHash  cdssdk.FileHash
	Output    exec.VarID
}

type HTTPRequestValue struct {
	types.HTTPRequest
}

func (v *HTTPRequestValue) Clone() exec.VarValue {
	return &HTTPRequestValue{
		HTTPRequest: v.HTTPRequest,
	}
}

func (o *BypassFromShardStoreHTTP) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	stgAgts, err := exec.GetValueByType[*agtpool.AgentPool](ctx)
	if err != nil {
		return err
	}

	shardStore, err := stgAgts.GetShardStore(o.StorageID)
	if err != nil {
		return err
	}

	br, ok := shardStore.(types.HTTPBypassRead)
	if !ok {
		return fmt.Errorf("shard store %v not support bypass read", o.StorageID)
	}

	req, err := br.HTTPBypassRead(o.FileHash)
	if err != nil {
		return err
	}

	e.PutVar(o.Output, &HTTPRequestValue{HTTPRequest: req})
	return nil
}

func (o *BypassFromShardStoreHTTP) String() string {
	return fmt.Sprintf("BypassFromShardStoreHTTP[StorageID:%v] FileHash: %v, Output: %v", o.StorageID, o.FileHash, o.Output)
}

// 旁路写入
type BypassToShardStoreNode struct {
	dag.NodeBase
	StorageID        cdssdk.StorageID
	FileHashStoreKey string
}

func (b *GraphNodeBuilder) NewBypassToShardStore(storageID cdssdk.StorageID, fileHashStoreKey string) *BypassToShardStoreNode {
	node := &BypassToShardStoreNode{
		StorageID:        storageID,
		FileHashStoreKey: fileHashStoreKey,
	}
	b.AddNode(node)

	node.InputValues().Init(1)
	node.OutputValues().Init(node, 2)
	return node
}

func (n *BypassToShardStoreNode) BypassFileInfoSlot() dag.ValueInputSlot {
	return dag.ValueInputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *BypassToShardStoreNode) BypassCallbackVar() dag.ValueOutputSlot {
	return dag.ValueOutputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *BypassToShardStoreNode) FileHashVar() dag.ValueOutputSlot {
	return dag.ValueOutputSlot{
		Node:  n,
		Index: 1,
	}
}

func (t *BypassToShardStoreNode) GenerateOp() (exec.Op, error) {
	return &BypassToShardStore{
		StorageID:      t.StorageID,
		BypassFileInfo: t.BypassFileInfoSlot().Var().VarID,
		BypassCallback: t.BypassCallbackVar().Var().VarID,
		FileHash:       t.FileHashVar().Var().VarID,
	}, nil
}

// 旁路读取
type BypassFromShardStoreNode struct {
	dag.NodeBase
	StorageID cdssdk.StorageID
	FileHash  cdssdk.FileHash
}

func (b *GraphNodeBuilder) NewBypassFromShardStore(storageID cdssdk.StorageID, fileHash cdssdk.FileHash) *BypassFromShardStoreNode {
	node := &BypassFromShardStoreNode{
		StorageID: storageID,
		FileHash:  fileHash,
	}
	b.AddNode(node)

	node.OutputValues().Init(node, 1)
	return node
}

func (n *BypassFromShardStoreNode) FilePathVar() dag.ValueOutputSlot {
	return dag.ValueOutputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *BypassFromShardStoreNode) GenerateOp() (exec.Op, error) {
	return &BypassFromShardStore{
		StorageID: n.StorageID,
		FileHash:  n.FileHash,
		Output:    n.FilePathVar().Var().VarID,
	}, nil
}

// 旁路Http读取
type BypassFromShardStoreHTTPNode struct {
	dag.NodeBase
	StorageID cdssdk.StorageID
	FileHash  cdssdk.FileHash
}

func (b *GraphNodeBuilder) NewBypassFromShardStoreHTTP(storageID cdssdk.StorageID, fileHash cdssdk.FileHash) *BypassFromShardStoreHTTPNode {
	node := &BypassFromShardStoreHTTPNode{
		StorageID: storageID,
		FileHash:  fileHash,
	}
	b.AddNode(node)

	node.OutputValues().Init(node, 1)
	return node
}

func (n *BypassFromShardStoreHTTPNode) HTTPRequestVar() dag.ValueOutputSlot {
	return dag.ValueOutputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *BypassFromShardStoreHTTPNode) GenerateOp() (exec.Op, error) {
	return &BypassFromShardStoreHTTP{
		StorageID: n.StorageID,
		FileHash:  n.FileHash,
		Output:    n.HTTPRequestVar().Var().VarID,
	}, nil
}
