package ops2

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/utils/io2"
)

func init() {
	exec.UseOp[*FileRead]()
	exec.UseOp[*FileWrite]()
}

type FileWrite struct {
	Input    *exec.StreamVar `json:"input"`
	FilePath string          `json:"filePath"`
}

func (o *FileWrite) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	dir := path.Dir(o.FilePath)
	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	file, err := os.Create(o.FilePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, o.Input.Stream)
	if err != nil {
		return fmt.Errorf("copying data to file: %w", err)
	}

	return nil
}

func (o *FileWrite) String() string {
	return fmt.Sprintf("FileWrite %v -> %s", o.Input.ID, o.FilePath)
}

type FileRead struct {
	Output   *exec.StreamVar `json:"output"`
	FilePath string          `json:"filePath"`
}

func (o *FileRead) Execute(ctx context.Context, e *exec.Executor) error {
	file, err := os.Open(o.FilePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}

	fut := future.NewSetVoid()
	o.Output.Stream = io2.AfterReadClosed(file, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	e.PutVars(o.Output)
	fut.Wait(ctx)

	return nil
}

func (o *FileRead) String() string {
	return fmt.Sprintf("FileRead %s -> %v", o.FilePath, o.Output.ID)
}

type FileReadNode struct {
	dag.NodeBase
	FilePath string
}

func (b *GraphNodeBuilder) NewFileRead(filePath string) *FileReadNode {
	node := &FileReadNode{
		FilePath: filePath,
	}
	b.AddNode(node)
	node.OutputStreams().SetupNew(node, b.NewStreamVar())
	return node
}

func (t *FileReadNode) Output() dag.StreamSlot {
	return dag.StreamSlot{
		Var:   t.OutputStreams().Get(0),
		Index: 0,
	}
}

func (t *FileReadNode) GenerateOp() (exec.Op, error) {
	return &FileRead{
		Output:   t.OutputStreams().Get(0).Var,
		FilePath: t.FilePath,
	}, nil
}

// func (t *FileReadType) String() string {
// 	return fmt.Sprintf("FileRead[%s]%v%v", t.FilePath, formatStreamIO(node), formatValueIO(node))
// }

type FileWriteNode struct {
	dag.NodeBase
	FilePath string
}

func (b *GraphNodeBuilder) NewFileWrite(filePath string) *FileWriteNode {
	node := &FileWriteNode{
		FilePath: filePath,
	}
	b.AddNode(node)
	return node
}

func (t *FileWriteNode) Input() dag.StreamSlot {
	return dag.StreamSlot{
		Var:   t.InputStreams().Get(0),
		Index: 0,
	}
}

func (t *FileWriteNode) SetInput(str *dag.StreamVar) {
	t.InputStreams().EnsureSize(1)
	str.Connect(t, 0)
}

func (t *FileWriteNode) GenerateOp() (exec.Op, error) {
	return &FileWrite{
		Input:    t.InputStreams().Get(0).Var,
		FilePath: t.FilePath,
	}, nil
}
