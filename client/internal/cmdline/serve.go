package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/storage/client/internal/config"
	"gitlink.org.cn/cloudream/storage/client/internal/http"
)

// ServeHTTP 启动HTTP服务。
// ctx: 命令行上下文，包含服务配置等信息。
// args: 命令行参数，第一个参数可选地指定HTTP服务器监听地址。
// 返回值: 如果启动过程中遇到错误，返回错误信息；否则返回nil。
func ServeHTTP(ctx CommandContext, args []string) error {
	// 默认监听地址为":7890"，如果提供了命令行参数，则使用参数指定的地址。
	listenAddr := ":7890"
	if len(args) > 0 {
		listenAddr = args[0]
	}

	awsAuth, err := http.NewAWSAuth(config.Cfg().AuthAccessKey, config.Cfg().AuthSecretKey)
	if err != nil {
		return fmt.Errorf("new aws auth: %w", err)
	}

	// 创建一个新的HTTP服务器实例。
	httpSvr, err := http.NewServer(listenAddr, ctx.Cmdline.Svc, awsAuth)
	if err != nil {
		return fmt.Errorf("new http server: %w", err)
	}

	// 启动HTTP服务。
	err = httpSvr.Serve()
	if err != nil {
		return fmt.Errorf("serving http: %w", err)
	}

	return nil
}

// 初始化函数，将ServeHTTP命令注册到命令列表中。
func init() {
	commands.MustAdd(ServeHTTP, "serve", "http")
}
