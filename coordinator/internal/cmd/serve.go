package cmd

import (
	"context"
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gitlink.org.cn/cloudream/storage/coordinator/internal/config"
	mymq "gitlink.org.cn/cloudream/storage/coordinator/internal/mq"
)

func serve(configPath string) {
	err := config.Init(configPath)
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	err = logger.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	db2, err := db2.NewDB(&config.Cfg().DB)
	if err != nil {
		logger.Fatalf("new db2 failed, err: %s", err.Error())
	}

	// 初始化系统事件发布器
	evtPub, err := sysevent.NewPublisher(sysevent.ConfigFromMQConfig(config.Cfg().RabbitMQ), &stgmod.SourceCoordinator{})
	if err != nil {
		logger.Errorf("new sysevent publisher: %v", err)
		os.Exit(1)
	}
	go servePublisher(evtPub)

	coorSvr, err := coormq.NewServer(mymq.NewService(db2, evtPub), config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new coordinator server failed, err: %s", err.Error())
	}

	coorSvr.OnError(func(err error) {
		logger.Warnf("coordinator server err: %s", err.Error())
	})

	// 启动服务
	go serveCoorServer(coorSvr, config.Cfg().RabbitMQ)

	forever := make(chan bool)
	<-forever
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

func serveCoorServer(server *coormq.Server, cfg mq.Config) {
	logger.Info("start serving command server")

	ch := server.Start(cfg)
	if ch == nil {
		logger.Errorf("RabbitMQ logEvent is nil")
		os.Exit(1)
	}

loop:
	for {
		val, err := ch.Receive()
		if err != nil {
			logger.Errorf("command server stopped with error: %s", err.Error())
			break
		}

		switch val := val.(type) {
		case error:
			logger.Errorf("rabbitmq connect with error: %v", val)
		case mq.ServerExit:
			if val.Error != nil {
				logger.Errorf("rabbitmq server exit with error: %v", val.Error)
			} else {
				logger.Info("rabbitmq server exit")
			}
			break loop
		}
	}
	logger.Info("command server stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}
