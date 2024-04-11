package main

// 主程序包，负责初始化配置、日志、数据库连接、分布式锁、事件执行器、扫描器服务器和定时任务。
import (
	"fmt"
	"os"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
	"gitlink.org.cn/cloudream/storage/scanner/internal/config"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/mq"
	"gitlink.org.cn/cloudream/storage/scanner/internal/tickevent"
)

func main() {
	// 初始化配置
	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	// 初始化日志
	err = logger.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	// 初始化数据库连接
	db, err := db.NewDB(&config.Cfg().DB)
	if err != nil {
		logger.Fatalf("new db failed, err: %s", err.Error())
	}

	// 初始化消息队列连接池
	stgglb.InitMQPool(&config.Cfg().RabbitMQ)

	// 同步等待组，用于等待所有Go协程完成
	wg := sync.WaitGroup{}
	wg.Add(3)

	// 初始化分布式锁服务
	distlockSvc, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}
	go serveDistLock(distlockSvc, &wg)

	// 初始化事件执行器，并启动服务
	eventExecutor := event.NewExecutor(db, distlockSvc)
	go serveEventExecutor(&eventExecutor, &wg)

	// 初始化扫描器服务器，并启动服务
	agtSvr, err := scmq.NewServer(mq.NewService(&eventExecutor), &config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError(func(err error) {
		logger.Warnf("agent server err: %s", err.Error())
	})

	go serveScannerServer(agtSvr, &wg)

	// 初始化并启动定时任务
	tickExecutor := tickevent.NewExecutor(tickevent.ExecuteArgs{
		EventExecutor: &eventExecutor,
		DB:            db,
	})
	startTickEvent(&tickExecutor)

	// 等待所有服务完成
	wg.Wait()
}

// serveEventExecutor 启动事件执行器服务
// executor: 事件执行器实例
// wg: 同步等待组
func serveEventExecutor(executor *event.Executor, wg *sync.WaitGroup) {
	logger.Info("start serving event executor")

	err := executor.Execute()

	if err != nil {
		logger.Errorf("event executor stopped with error: %s", err.Error())
	}

	logger.Info("event executor stopped")

	wg.Done()
}

// serveScannerServer 启动扫描器服务器服务
// server: 扫描器服务器实例
// wg: 同步等待组
func serveScannerServer(server *scmq.Server, wg *sync.WaitGroup) {
	logger.Info("start serving scanner server")

	err := server.Serve()

	if err != nil {
		logger.Errorf("scanner server stopped with error: %s", err.Error())
	}

	logger.Info("scanner server stopped")

	wg.Done()
}

// serveDistLock 启动分布式锁服务
// svc: 分布式锁服务实例
// wg: 同步等待组
func serveDistLock(svc *distlock.Service, wg *sync.WaitGroup) {
	logger.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		logger.Errorf("distlock stopped with error: %s", err.Error())
	}

	logger.Info("distlock stopped")

	wg.Done()
}

// startTickEvent 启动定时任务事件。
// 参数 tickExecutor 为 ticket 事件执行器的指针，用于启动各种定时任务。
func startTickEvent(tickExecutor *tickevent.Executor) {
	// 考虑增加配置文件来配置这些任务的间隔时间

	interval := 5 * 60 * 1000 // 定义默认的任务执行间隔时间

	// 启动所有 Agent 检查缓存的定时任务
	tickExecutor.Start(tickevent.NewBatchAllAgentCheckCache(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	// 启动检查所有包的定时任务
	tickExecutor.Start(tickevent.NewBatchCheckAllPackage(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	// 注释掉的代码块，可能是未来可能使用的任务，目前未启用

	// 启动检查所有存储的定时任务
	tickExecutor.Start(tickevent.NewBatchCheckAllStorage(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	// 启动检查 Agent 状态的定时任务，此任务的执行间隔与上述任务不同
	tickExecutor.Start(tickevent.NewCheckAgentState(), 5*60*1000, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	// 启动检查包冗余的定时任务
	tickExecutor.Start(tickevent.NewBatchCheckPackageRedundancy(), interval, tickevent.StartOption{RandomStartDelayMs: 20 * 60 * 1000})

	// 启动清理固定项目的定时任务
	tickExecutor.Start(tickevent.NewBatchCleanPinned(), interval, tickevent.StartOption{RandomStartDelayMs: 20 * 60 * 1000})
}
