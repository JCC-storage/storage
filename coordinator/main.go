package main

// 主程序包，负责初始化和启动协调器服务器。
import (
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	mydb "gitlink.org.cn/cloudream/storage/common/pkgs/db"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
	"gitlink.org.cn/cloudream/storage/coordinator/internal/config"
	"gitlink.org.cn/cloudream/storage/coordinator/internal/mq"
)

// 主函数，负责程序的初始化和启动。
func main() {
	// 初始化配置
	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	// 初始化日志系统
	err = logger.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	// 初始化数据库连接
	db, err := mydb.NewDB(&config.Cfg().DB)
	if err != nil {
		logger.Fatalf("new db failed, err: %s", err.Error())
	}

	// 初始化扫描器客户端
	scanner, err := scmq.NewClient(&config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new scanner client failed, err: %s", err.Error())
	}

	// 初始化协调器服务器
	coorSvr, err := coormq.NewServer(mq.NewService(db, scanner), &config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new coordinator server failed, err: %s", err.Error())
	}

	// 设置协调器服务器错误处理
	coorSvr.OnError(func(err error) {
		logger.Warnf("coordinator server err: %s", err.Error())
	})

	// 启动协调器服务器为异步操作
	go serveCoorServer(coorSvr)

	// 永久等待，保持程序运行
	forever := make(chan bool)
	<-forever
}

// serveCoorServer 启动并运行协调器服务器。
func serveCoorServer(server *coormq.Server) {
	logger.Info("start serving command server")

	// 服务启动和错误处理
	err := server.Serve()
	if err != nil {
		logger.Errorf("command server stopped with error: %s", err.Error())
	}

	logger.Info("command server stopped")
}
