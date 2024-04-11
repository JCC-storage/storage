package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/cmdtrie"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

// parseScannerEventCmdTrie 是一个静态命令 trie 树，用于解析扫描器事件命令。
var parseScannerEventCmdTrie cmdtrie.StaticCommandTrie[any] = cmdtrie.NewStaticCommandTrie[any]()

// ScannerPostEvent 发布扫描器事件。
// ctx: 命令上下文。
// args: 命令参数数组。
// 返回值: 执行错误时返回 error。
func ScannerPostEvent(ctx CommandContext, args []string) error {
	// 尝试执行解析扫描器事件命令。
	ret, err := parseScannerEventCmdTrie.Execute(args, cmdtrie.ExecuteOption{ReplaceEmptyArrayWithNil: true})
	if err != nil {
		// 解析失败，返回错误信息。
		return fmt.Errorf("execute parsing event command failed, err: %w", err)
	}

	// 发布解析得到的事件。
	err = ctx.Cmdline.Svc.ScannerSvc().PostEvent(ret.(scevt.Event), false, false)
	if err != nil {
		// 发布事件失败，返回错误信息。
		return fmt.Errorf("post event to scanner failed, err: %w", err)
	}

	return nil
}

// 初始化函数，用于向 parseScannerEventCmdTrie 注册扫描器事件命令。
func init() {
	// 注册 AgentCacheGC 事件。
	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCacheGC, myreflect.TypeNameOf[scevt.AgentCacheGC]())

	// 注册 AgentCheckCache 事件。
	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckCache, myreflect.TypeNameOf[scevt.AgentCheckCache]())

	// 注册 AgentCheckState 事件。
	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckState, myreflect.TypeNameOf[scevt.AgentCheckState]())

	// 注册 AgentStorageGC 事件。
	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentStorageGC, myreflect.TypeNameOf[scevt.AgentStorageGC]())

	// 注册 AgentCheckStorage 事件。
	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckStorage, myreflect.TypeNameOf[scevt.AgentCheckStorage]())

	// 注册 CheckPackage 事件。
	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckPackage, myreflect.TypeNameOf[scevt.CheckPackage]())

	// 注册 CheckPackageRedundancy 事件。
	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckPackageRedundancy, myreflect.TypeNameOf[scevt.CheckPackageRedundancy]())

	// 注册 CleanPinned 事件。
	parseScannerEventCmdTrie.MustAdd(scevt.NewCleanPinned, myreflect.TypeNameOf[scevt.CleanPinned]())

	// 向命令行注册 ScannerPostEvent 命令。
	commands.MustAdd(ScannerPostEvent, "scanner", "event")
}
