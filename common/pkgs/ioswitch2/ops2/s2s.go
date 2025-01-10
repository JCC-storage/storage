package ops2

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/factory"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

func init() {
	exec.UseOp[*S2STransfer]()
}

type S2STransfer struct {
	Src            stgmod.StorageDetail
	SrcPath        exec.VarID
	Dst            stgmod.StorageDetail
	Output         exec.VarID
	BypassCallback exec.VarID
}

func (o *S2STransfer) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	srcPath, err := exec.BindVar[*BypassFilePathValue](e, ctx.Context, o.SrcPath)
	if err != nil {
		return err
	}

	s2s, err := factory.GetBuilder(o.Dst).CreateS2STransfer()
	if err != nil {
		return err
	}

	// 传输文件
	dstPath, err := s2s.Transfer(ctx.Context, o.Src, srcPath.Path)
	if err != nil {
		return err
	}
	defer s2s.Abort()

	// 告知后续Op处理临时文件
	e.PutVar(o.Output, &BypassFileInfoValue{BypassFileInfo: types.BypassFileInfo{
		TempFilePath: dstPath,
		FileHash:     srcPath.Info.Hash,
		Size:         srcPath.Info.Size,
	}})

	// 等待后续Op处理临时文件
	cb, err := exec.BindVar[*BypassHandleResultValue](e, ctx.Context, o.BypassCallback)
	if err != nil {
		return fmt.Errorf("getting temp file callback: %v", err)
	}

	if cb.Commited {
		s2s.Complete()
	}

	return nil
}

func (o *S2STransfer) String() string {
	return fmt.Sprintf("S2STransfer(%v@%v -> %v:%v)", o.Src.Storage.String(), o.SrcPath, o.Dst.Storage.String(), o.Output)
}

type S2STransferNode struct {
	dag.NodeBase
	Src stgmod.StorageDetail
	Dst stgmod.StorageDetail
}

func (b *GraphNodeBuilder) NewS2STransfer(src stgmod.StorageDetail, dst stgmod.StorageDetail) *S2STransferNode {
	n := &S2STransferNode{
		Src: src,
		Dst: dst,
	}

	n.OutputValues().Init(n, 1)
	n.InputValues().Init(2)

	return n
}

func (n *S2STransferNode) SrcPathSlot() dag.ValueInputSlot {
	return dag.ValueInputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *S2STransferNode) BypassCallbackSlot() dag.ValueInputSlot {
	return dag.ValueInputSlot{
		Node:  n,
		Index: 1,
	}
}

func (n *S2STransferNode) BypassFileInfoVar() dag.ValueOutputSlot {
	return dag.ValueOutputSlot{
		Node:  n,
		Index: 0,
	}
}

func (n *S2STransferNode) GenerateOp() (exec.Op, error) {
	return &S2STransfer{
		Src:            n.Src,
		SrcPath:        n.SrcPathSlot().Var().VarID,
		Dst:            n.Dst,
		Output:         n.BypassFileInfoVar().Var().VarID,
		BypassCallback: n.BypassCallbackSlot().Var().VarID,
	}, nil
}
