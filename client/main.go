package main

import (
	"fmt"
	"os"

	_ "google.golang.org/grpc/balancer/grpclb"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/client/internal/cmdline"
	"gitlink.org.cn/cloudream/storage/client/internal/config"
	"gitlink.org.cn/cloudream/storage/client/internal/services"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
)

/*
该Go程序是一个客户端应用程序，主要负责初始化配置、日志、全局变量，并启动网络检测、分布式锁服务、任务管理器和服务处理客户端请求。具体功能如下：
程序的主入口函数main()：
初始化配置，如果失败则结束进程。
初始化日志系统，如果失败则结束进程。
初始化全局变量，包括本地配置、消息队列池和Agent RPC池。
根据IPFS配置初始化IPFS客户端。
启动网络连通性检测。
启动分布式锁服务，并在独立的goroutine中运行。
创建任务管理器。
创建服务实例。
创建命令行接口。
分发命令行指令。
辅助函数serveDistLock()：
在独立的goroutine中启动分布式锁服务。
处理服务停止时的错误。
该程序使用了多个外部包和模块，包括配置管理、日志系统、全局变量初始化、网络检测、分布式锁服务、任务管理和命令行接口等。这些模块共同协作，提供了一个功能丰富的客户端应用程序。
*/

// @Description: 程序的主入口函数，负责初始化配置、日志、全局变量，并启动网络检测、分布式锁服务、任务管理器和服务处理客户端请求。
func main() {
	// 初始化配置，失败则结束进程
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

	// 初始化全局变量
	stgglb.InitLocal(&config.Cfg().Local)
	stgglb.InitMQPool(&config.Cfg().RabbitMQ)
	stgglb.InitAgentRPCPool(&config.Cfg().AgentGRPC)
	// 如果IPFS配置非空，初始化IPFS客户端
	if config.Cfg().IPFS != nil {
		logger.Infof("IPFS config is not empty, so create a ipfs client")
		stgglb.InitIPFSPool(config.Cfg().IPFS)
	}

	// 启动网络连通性检测
	conCol := connectivity.NewCollector(&config.Cfg().Connectivity, nil)
	conCol.CollectInPlace()

	// 启动分布式锁服务
	distlockSvc, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}
	go serveDistLock(distlockSvc) // 在goroutine中运行分布式锁服务

	// 创建任务管理器
	taskMgr := task.NewManager(distlockSvc, &conCol)

	// 创建服务实例
	svc, err := services.NewService(distlockSvc, &taskMgr)
	if err != nil {
		logger.Warnf("new services failed, err: %s", err.Error())
		os.Exit(1)
	}

	// 创建命令行接口
	cmds, err := cmdline.NewCommandline(svc)
	if err != nil {
		logger.Warnf("new command line failed, err: %s", err.Error())
		os.Exit(1)
	}

	// 分发命令行指令
	cmds.DispatchCommand(os.Args[1:])
}

// serveDistLock 启动分布式锁服务
//
// @Description: 在独立的goroutine中启动分布式锁服务，并处理服务停止时的错误。
func serveDistLock(svc *distlock.Service) {
	logger.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		logger.Errorf("distlock stopped with error: %s", err.Error())
	}

	logger.Info("distlock stopped")
}
