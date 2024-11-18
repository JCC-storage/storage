package ops2

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/mgr"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

type InternalFaaSGalMultiply struct {
	Coefs            [][]byte         `json:"coefs"`
	InputFilePathes  []exec.VarID     `json:"inputFilePathes"`  // 输入的文件的路径
	OutputFilePathes []exec.VarID     `json:"outputFilePathes"` // 输出的文件的路径
	ChunkSize        int              `json:"chunkSize"`
	StorageID        cdssdk.StorageID `json:"storageID"`
}

func (o *InternalFaaSGalMultiply) Execute(ctx *exec.ExecContext, e *exec.Executor) error {
	stgMgr, err := exec.GetValueByType[*mgr.Manager](ctx)
	if err != nil {
		return err
	}

	fass, err := mgr.GetComponent[types.InternalFaaSCall](stgMgr, o.StorageID)
	if err != nil {
		return fmt.Errorf("getting faas component: %w", err)
	}

	tmp, err := mgr.GetComponent[types.TempStore](stgMgr, o.StorageID)
	if err != nil {
		return fmt.Errorf("getting temp store component: %w", err)
	}

	inputVars, err := exec.BindArray[*exec.StringValue](e, ctx.Context, o.InputFilePathes)
	if err != nil {
		return err
	}

	var outputs []string
	for i := 0; i < len(o.OutputFilePathes); i++ {
		outputs = append(outputs, tmp.CreateTemp())
	}
	var outputVars []*exec.StringValue
	for _, output := range outputs {
		outputVars = append(outputVars, &exec.StringValue{Value: output})
	}

	inputs := lo.Map(inputVars, func(v *exec.StringValue, idx int) string { return v.Value })

	err = fass.GalMultiply(ctx.Context, o.Coefs, inputs, outputs, o.ChunkSize)
	if err != nil {
		return fmt.Errorf("faas gal multiply: %w", err)
	}

	exec.PutArray(e, o.OutputFilePathes, outputVars)
	return nil
}
