package main

import (
	"context"
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gitlink.org.cn/cloudream/storage/scanner/internal/config"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/mq"
	"gitlink.org.cn/cloudream/storage/scanner/internal/tickevent"
)

func main() {
	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	err = logger.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	db, err := db2.NewDB(&config.Cfg().DB)
	if err != nil {
		logger.Fatalf("new db failed, err: %s", err.Error())
	}

	stgglb.InitMQPool(config.Cfg().RabbitMQ)

	stgglb.InitAgentRPCPool(&agtrpc.PoolConfig{})

	// 启动分布式锁服务
	distlockSvc, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}
	go serveDistLock(distlockSvc)

	// 启动存储服务管理器
	stgAgts := agtpool.NewPool()

	// 初始化系统事件发布器
	evtPub, err := sysevent.NewPublisher(sysevent.ConfigFromMQConfig(config.Cfg().RabbitMQ), &stgmod.SourceScanner{})
	if err != nil {
		logger.Errorf("new sysevent publisher: %v", err)
		os.Exit(1)
	}
	go servePublisher(evtPub)

	// 启动事件执行器
	eventExecutor := event.NewExecutor(db, distlockSvc, stgAgts, evtPub)
	go serveEventExecutor(&eventExecutor)

	agtSvr, err := scmq.NewServer(mq.NewService(&eventExecutor), config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError(func(err error) {
		logger.Warnf("agent server err: %s", err.Error())
	})
	go serveScannerServer(agtSvr)

	tickExecutor := tickevent.NewExecutor(tickevent.ExecuteArgs{
		EventExecutor: &eventExecutor,
		DB:            db,
	})
	startTickEvent(&tickExecutor)

	forever := make(chan struct{})
	<-forever
}

func serveEventExecutor(executor *event.Executor) {
	logger.Info("start serving event executor")

	err := executor.Execute()

	if err != nil {
		logger.Errorf("event executor stopped with error: %s", err.Error())
	}

	logger.Info("event executor stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func servePublisher(evtPub *sysevent.Publisher) {
	logger.Info("start serving sysevent publisher")

	ch := evtPub.Start()

loop:
	for {
		val, err := ch.Receive().Wait(context.Background())
		if err != nil {
			logger.Errorf("sysevent publisher stopped with error: %s", err.Error())
			break
		}

		switch val := val.(type) {
		case sysevent.PublishError:
			logger.Errorf("publishing event: %v", val)

		case sysevent.PublisherExited:
			if val.Err != nil {
				logger.Errorf("publisher exited with error: %v", val.Err)
			} else {
				logger.Info("publisher exited")
			}
			break loop

		case sysevent.OtherError:
			logger.Errorf("sysevent: %v", val)
		}
	}
	logger.Info("sysevent publisher stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func serveScannerServer(server *scmq.Server) {
	logger.Info("start serving scanner server")

	ch := server.Start()
	if ch == nil {
		logger.Errorf("RabbitMQ logEvent is nil")
		os.Exit(1)
	}

	for {
		val, err := ch.Receive()
		if err != nil {
			logger.Errorf("command server stopped with error: %s", err.Error())
			break
		}

		switch val := val.(type) {
		case error:
			logger.Errorf("rabbitmq connect with error: %v", val)
		case int:
			if val == 1 {
				break
			}
		}
	}
	logger.Info("command server stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func serveDistLock(svc *distlock.Service) {
	logger.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		logger.Errorf("distlock stopped with error: %s", err.Error())
	}

	logger.Info("distlock stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func startTickEvent(tickExecutor *tickevent.Executor) {
	// TODO 可以考虑增加配置文件，配置这些任务间隔时间

	interval := 5 * 60 * 1000

	tickExecutor.Start(tickevent.NewBatchAllAgentCheckShardStore(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewStorageGC(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewCheckAgentState(), 5*60*1000, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewBatchCheckPackageRedundancy(), interval, tickevent.StartOption{RandomStartDelayMs: 20 * 60 * 1000})

	tickExecutor.Start(tickevent.NewBatchCleanPinned(), interval, tickevent.StartOption{RandomStartDelayMs: 20 * 60 * 1000})

	tickExecutor.Start(tickevent.NewUpdateAllPackageAccessStatAmount(), interval, tickevent.StartOption{RandomStartDelayMs: 20 * 60 * 1000})
}
