package ops2

import (
	"encoding/json"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/sdks/cloudstorage"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func init() {
	exec.UseOp[*MultipartManage]()
	exec.UseOp[*MultipartUpload]()
	exec.UseVarValue[*InitUploadValue]()
}

type InitUploadValue struct {
	UploadID string `json:"uploadID"`
}

func (v *InitUploadValue) Clone() exec.VarValue {
	return &*v
}

type MultipartManage struct {
	Address  cdssdk.StorageAddress `json:"address"`
	UploadID exec.VarID            `json:"uploadID"`
	ObjectID exec.VarID            `json:"objectID"`
}

func (o *MultipartManage) Execute(ctx *exec.ExecContext, e *exec.Executor) error {

	var oss cloudstorage.ObjectStorage

	switch addr := o.Address.(type) {
	case *cdssdk.LocalStorageAddress:
		err := json.Unmarshal([]byte(addr.String()), &oss)
		if err != nil {
			return err
		}
	}

	client, err := cloudstorage.NewObjectStorageClient(oss)
	if err != nil {
		return err
	}
	defer client.Close()

	uploadID, err := client.InitiateMultipartUpload("")
	if err != nil {
		return err
	}
	e.PutVar(o.UploadID, &InitUploadValue{UploadID: uploadID})

	objectID, err := client.CompleteMultipartUpload()
	if err != nil {
		return err
	}
	o.ObjectID.Value = objectID
	e.PutVars(o.ObjectID)

	return nil
}

func (o *MultipartManage) String() string {
	return "MultipartManage"
}

type MultipartManageNode struct {
	dag.NodeBase
	Address cdssdk.StorageAddress
}

func (b *GraphNodeBuilder) NewMultipartManage(addr cdssdk.StorageAddress) *MultipartManageNode {
	node := &MultipartManageNode{
		Address: addr,
	}
	b.AddNode(node)
	return node
}

func (t *MultipartManageNode) GenerateOp() (exec.Op, error) {
	return &MultipartManage{
		Address: t.Address,
	}, nil
}

type MultipartUpload struct {
	Address cdssdk.StorageAddress `json:"address"`
	FileMD5 *exec.StringVar       `json:"fileMD5"`
}

func (o *MultipartUpload) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	var oss cloudstorage.ObjectStorage
	switch addr := o.Address.(type) {
	case *cdssdk.LocalStorageAddress:
		err := json.Unmarshal([]byte(addr.String()), &oss)
		if err != nil {
			return err
		}
	}

	client, err := cloudstorage.NewObjectStorageClient(oss)
	if err != nil {
		return err
	}
	client.UploadPart()
	return nil
}

func (o *MultipartUpload) String() string {
	return "MultipartUpload"
}

type MultipartUploadNode struct {
	dag.NodeBase
	Address cdssdk.StorageAddress
}

func (b *GraphNodeBuilder) NewMultipartUpload(addr cdssdk.StorageAddress) *MultipartUploadNode {
	node := &MultipartUploadNode{
		Address: addr,
	}
	b.AddNode(node)
	return node
}

func (t MultipartUploadNode) GenerateOp() (exec.Op, error) {
	return &MultipartUpload{
		Address: t.Address,
	}, nil
}
