package ops2

import (
	"fmt"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/cos"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/mgr"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/obs"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/oss"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"io"
	"time"
)

func init() {
	exec.UseOp[*MultipartManage]()
	exec.UseOp[*MultipartUpload]()
	exec.UseVarValue[*InitUploadValue]()
}

type InitUploadValue struct {
	Key      string `xml:"Key"`      // Object name to upload
	UploadID string `xml:"UploadId"` // Generated UploadId
}

func (v *InitUploadValue) Clone() exec.VarValue {
	return &*v
}

type MultipartManage struct {
	Address      cdssdk.StorageAddress `json:"address"`
	UploadArgs   exec.VarID            `json:"uploadArgs"`
	UploadOutput exec.VarID            `json:"uploadOutput"`
	StorageID    cdssdk.StorageID      `json:"storageID"`
}

func (o *MultipartManage) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	manager, err := exec.GetValueByType[*mgr.Manager](ctx)
	if err != nil {
		return err
	}

	var client types.MultipartUploader
	switch addr := o.Address.(type) {
	case *cdssdk.OSSAddress:
		client = oss.NewMultiPartUpload(addr)
	case *cdssdk.OBSAddress:
		client = obs.NewMultiPartUpload(addr)
	case *cdssdk.COSAddress:
		client = cos.NewMultiPartUpload(addr)
	}
	defer client.Close()

	tempStore, err := manager.GetTempStore(o.StorageID)
	if err != nil {
		return err
	}
	objName := tempStore.CreateTemp()

	uploadID, err := client.InitiateMultipartUpload(objName)
	if err != nil {
		return err
	}
	e.PutVar(o.UploadArgs, &InitUploadValue{
		UploadID: uploadID,
		Key:      objName,
	})

	parts, err := exec.BindVar[*UploadPartOutputValue](e, ctx.Context, o.UploadOutput)
	if err != nil {
		return err
	}
	err = client.CompleteMultipartUpload(uploadID, objName, parts.Parts)
	if err != nil {
		return err
	}

	return nil
}

func (o *MultipartManage) String() string {
	return "MultipartManage"
}

type MultipartManageNode struct {
	dag.NodeBase
	Address   cdssdk.StorageAddress
	StorageID cdssdk.StorageID `json:"storageID"`
}

func (b *GraphNodeBuilder) NewMultipartManage(addr cdssdk.StorageAddress, storageID cdssdk.StorageID) *MultipartManageNode {
	node := &MultipartManageNode{
		Address:   addr,
		StorageID: storageID,
	}
	b.AddNode(node)
	return node
}

func (t *MultipartManageNode) GenerateOp() (exec.Op, error) {
	return &MultipartManage{
		Address:   t.Address,
		StorageID: t.StorageID,
	}, nil
}

type MultipartUpload struct {
	Address      cdssdk.StorageAddress `json:"address"`
	UploadArgs   exec.VarID            `json:"uploadArgs"`
	UploadOutput exec.VarID            `json:"uploadOutput"`
	PartNumbers  []int                 `json:"partNumbers"`
	PartSize     []int64               `json:"partSize"`
	Input        exec.VarID            `json:"input"`
}

type UploadPartOutputValue struct {
	Parts []*types.UploadPartOutput `json:"parts"`
}

func (v *UploadPartOutputValue) Clone() exec.VarValue {
	return &*v
}

func (o *MultipartUpload) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	initUploadResult, err := exec.BindVar[*InitUploadValue](e, ctx.Context, o.UploadArgs)
	if err == nil {
		return err
	}

	input, err := exec.BindVar[*exec.StreamValue](e, ctx.Context, o.Input)
	if err != nil {
		return err
	}
	defer input.Stream.Close()

	var client types.MultipartUploader
	switch addr := o.Address.(type) {
	case *cdssdk.OSSAddress:
		client = oss.NewMultiPartUpload(addr)
	}

	var parts UploadPartOutputValue
	for i := 0; i < len(o.PartNumbers); i++ {
		startTime := time.Now()
		uploadPart, err := client.UploadPart(initUploadResult.UploadID, initUploadResult.Key, o.PartSize[i], o.PartNumbers[i], io.LimitReader(input.Stream, o.PartSize[i]))
		log.Debugf("upload multipart spend time: %v", time.Since(startTime))
		if err != nil {
			return fmt.Errorf("failed to upload part: %w", err)
		}
		parts.Parts = append(parts.Parts, uploadPart)
	}

	e.PutVar(o.UploadOutput, &parts)

	return nil
}

func (o *MultipartUpload) String() string {
	return "MultipartUpload"
}

type MultipartUploadNode struct {
	dag.NodeBase
	Address     cdssdk.StorageAddress
	PartNumbers []int   `json:"partNumbers"`
	PartSize    []int64 `json:"partSize"`
}

func (b *GraphNodeBuilder) NewMultipartUpload(addr cdssdk.StorageAddress, partNumbers []int, partSize []int64) *MultipartUploadNode {
	node := &MultipartUploadNode{
		Address:     addr,
		PartNumbers: partNumbers,
		PartSize:    partSize,
	}
	b.AddNode(node)
	return node
}

func (t MultipartUploadNode) GenerateOp() (exec.Op, error) {
	return &MultipartUpload{
		Address:     t.Address,
		PartNumbers: t.PartNumbers,
		PartSize:    t.PartSize,
	}, nil
}
