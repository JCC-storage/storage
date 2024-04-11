package cmdline

import (
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/cmdtrie"
	"gitlink.org.cn/cloudream/storage/client/internal/services"
)

// CommandContext 命令上下文，存储与命令行相关的上下文信息。
type CommandContext struct {
	Cmdline *Commandline // 指向当前的Commandline实例。
}

// commands 用于存储所有已注册的命令及其相关信息的Trie树。
var commands cmdtrie.CommandTrie[CommandContext, error] = cmdtrie.NewCommandTrie[CommandContext, error]()

// Commandline 命令行对象，封装了与服务交互的能力。
type Commandline struct {
	Svc *services.Service // 指向内部服务接口。
}

// NewCommandline 创建一个新的Commandline实例。
// svc: 指向内部服务的实例。
// 返回值: 初始化好的Commandline指针及可能的错误。
func NewCommandline(svc *services.Service) (*Commandline, error) {
	return &Commandline{
		Svc: svc,
	}, nil
}

// DispatchCommand 分发并执行命令。
// allArgs: 命令行中所有的参数。
// 功能: 根据参数执行相应的命令逻辑，出错时退出程序。
func (c *Commandline) DispatchCommand(allArgs []string) {
	cmdCtx := CommandContext{
		Cmdline: c,
	}
	// 执行命令，根据命令执行结果做相应处理。
	cmdErr, err := commands.Execute(cmdCtx, allArgs, cmdtrie.ExecuteOption{ReplaceEmptyArrayWithNil: true})
	if err != nil {
		fmt.Printf("execute command failed, err: %s", err.Error())
		os.Exit(1)
	}
	if cmdErr != nil {
		fmt.Printf("execute command failed, err: %s", cmdErr.Error())
		os.Exit(1)
	}
}

// MustAddCmd 必须添加命令。
// fn: 命令执行的函数。
// prefixWords: 命令的前缀词。
// 返回值: 无。
// 功能: 向命令树中添加命令，添加失败时会抛出异常。
func MustAddCmd(fn any, prefixWords ...string) any {
	commands.MustAdd(fn, prefixWords...)
	return nil
}
