package ops2

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func init() {
	exec.UseOp[*MultipartInitiator]()
	exec.UseOp[*MultipartUpload]()
	exec.UseVarValue[*MultipartUploadArgsValue]()
	exec.UseVarValue[*UploadedPartInfoValue]()
}

type MultipartUploadArgsValue struct {
	InitState types.MultipartInitState
}

func (v *MultipartUploadArgsValue) Clone() exec.VarValue {
	return &MultipartUploadArgsValue{
		InitState: v.InitState,
	}
}

type UploadedPartInfoValue struct {
	types.UploadedPartInfo
}

func (v *UploadedPartInfoValue) Clone() exec.VarValue {
	return &UploadedPartInfoValue{
		UploadedPartInfo: v.UploadedPartInfo,
	}
}

type MultipartInitiator struct {
	Storage          stgmod.StorageDetail
	UploadArgs       exec.VarID
	UploadedParts    []exec.VarID
	BypassFileOutput exec.VarID // 分片上传之后的临时文件的路径
	BypassCallback   exec.VarID // 临时文件使用结果，用于告知Initiator如何处理临时文件
}

func (o *MultipartInitiator) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	blder := factory.GetBuilder(o.Storage)
	if blder == nil {
		return fmt.Errorf("unsupported storage type: %T", o.Storage.Storage.Type)
	}

	initiator, err := blder.CreateMultipartInitiator(o.Storage)
	if err != nil {
		return err
	}
	defer initiator.Abort()

	// 启动一个新的上传任务
	initState, err := initiator.Initiate(ctx.Context)
	if err != nil {
		return err
	}
	// 分发上传参数
	e.PutVar(o.UploadArgs, &MultipartUploadArgsValue{
		InitState: initState,
	})

	// 收集分片上传结果
	partInfoValues, err := exec.BindArray[*UploadedPartInfoValue](e, ctx.Context, o.UploadedParts)
	if err != nil {
		return fmt.Errorf("getting uploaded parts: %v", err)
	}

	partInfos := make([]types.UploadedPartInfo, len(partInfoValues))
	for i, v := range partInfoValues {
		partInfos[i] = v.UploadedPartInfo
	}

	// 合并分片
	fileInfo, err := initiator.JoinParts(ctx.Context, partInfos)
	if err != nil {
		return fmt.Errorf("completing multipart upload: %v", err)
	}

	// 告知后续Op临时文件的路径
	e.PutVar(o.BypassFileOutput, &BypassFileInfoValue{
		BypassFileInfo: fileInfo,
	})

	// 等待后续Op处理临时文件
	cb, err := exec.BindVar[*BypassHandleResultValue](e, ctx.Context, o.BypassCallback)
	if err != nil {
		return fmt.Errorf("getting temp file callback: %v", err)
	}

	if cb.Commited {
		initiator.Complete()
	}

	return nil
}

func (o *MultipartInitiator) String() string {
	return fmt.Sprintf("MultipartInitiator Args: %v, Parts: %v, BypassFileOutput: %v, BypassCallback: %v", o.UploadArgs, o.UploadedParts, o.BypassFileOutput, o.BypassCallback)
}

type MultipartUpload struct {
	Storage      stgmod.StorageDetail
	UploadArgs   exec.VarID
	UploadResult exec.VarID
	PartStream   exec.VarID
	PartNumber   int
	PartSize     int64
}

func (o *MultipartUpload) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	blder := factory.GetBuilder(o.Storage)
	if blder == nil {
		return fmt.Errorf("unsupported storage type: %T", o.Storage.Storage.Type)
	}

	uploadArgs, err := exec.BindVar[*MultipartUploadArgsValue](e, ctx.Context, o.UploadArgs)
	if err != nil {
		return err
	}

	partStr, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.PartStream)
	if err != nil {
		return err
	}
	defer partStr.Stream.Close()

	uploader, err := blder.CreateMultipartUploader(o.Storage)
	if err != nil {
		return err
	}

	startTime := time.Now()
	uploadedInfo, err := uploader.UploadPart(ctx.Context, uploadArgs.InitState, o.PartSize, o.PartNumber, partStr.Stream)
	if err != nil {
		return err
	}
	log.Debugf("upload finished in %v", time.Since(startTime))

	e.PutVar(o.UploadResult, &UploadedPartInfoValue{
		uploadedInfo,
	})

	return nil
}

func (o *MultipartUpload) String() string {
	return fmt.Sprintf("MultipartUpload[PartNumber=%v,PartSize=%v] Args: %v, Result: %v, Stream: %v", o.PartNumber, o.PartSize, o.UploadArgs, o.UploadResult, o.PartStream)
}

type MultipartInitiatorNode struct {
	dag.NodeBase
	Storage stgmod.StorageDetail `json:"storageID"`
}

func (b *GraphNodeBuilder) NewMultipartInitiator(storage stgmod.StorageDetail) *MultipartInitiatorNode {
	node := &MultipartInitiatorNode{
		Storage: storage,
	}
	b.AddNode(node)

	node.OutputValues().Init(node, 2)
	node.InputValues().Init(1)
	return node
}

func (n *MultipartInitiatorNode) UploadArgsVar() *dag.ValueVar {
	return n.OutputValues().Get(0)
}

func (n *MultipartInitiatorNode) BypassFileInfoVar() *dag.ValueVar {
	return n.OutputValues().Get(1)
}

func (n *MultipartInitiatorNode) BypassCallbackSlot() dag.ValueInputSlot {
	return dag.ValueInputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *MultipartInitiatorNode) AppendPartInfoSlot() dag.ValueInputSlot {
	return dag.ValueInputSlot{
		Node:  n,
		Index: n.InputValues().EnlargeOne(),
	}
}

func (n *MultipartInitiatorNode) GenerateOp() (exec.Op, error) {
	return &MultipartInitiator{
		Storage:          n.Storage,
		UploadArgs:       n.UploadArgsVar().VarID,
		UploadedParts:    n.InputValues().GetVarIDsStart(1),
		BypassFileOutput: n.BypassFileInfoVar().VarID,
		BypassCallback:   n.BypassCallbackSlot().Var().VarID,
	}, nil
}

type MultipartUploadNode struct {
	dag.NodeBase
	Storage    stgmod.StorageDetail
	PartNumber int
	PartSize   int64
}

func (b *GraphNodeBuilder) NewMultipartUpload(stg stgmod.StorageDetail, partNumber int, partSize int64) *MultipartUploadNode {
	node := &MultipartUploadNode{
		Storage:    stg,
		PartNumber: partNumber,
		PartSize:   partSize,
	}
	b.AddNode(node)

	node.InputValues().Init(1)
	node.OutputValues().Init(node, 1)
	node.InputStreams().Init(1)
	return node
}

func (n *MultipartUploadNode) UploadArgsSlot() dag.ValueInputSlot {
	return dag.ValueInputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *MultipartUploadNode) UploadResultVar() *dag.ValueVar {
	return n.OutputValues().Get(0)
}

func (n *MultipartUploadNode) PartStreamSlot() dag.StreamInputSlot {
	return dag.StreamInputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *MultipartUploadNode) GenerateOp() (exec.Op, error) {
	return &MultipartUpload{
		Storage:      n.Storage,
		UploadArgs:   n.UploadArgsSlot().Var().VarID,
		UploadResult: n.UploadResultVar().VarID,
		PartStream:   n.PartStreamSlot().Var().VarID,
		PartNumber:   n.PartNumber,
		PartSize:     n.PartSize,
	}, nil
}
