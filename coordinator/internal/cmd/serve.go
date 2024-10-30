package cmd

import (
	"fmt"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	mydb "gitlink.org.cn/cloudream/storage/common/pkgs/db"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/coordinator/internal/config"
	"gitlink.org.cn/cloudream/storage/coordinator/internal/mq"
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

	db, err := mydb.NewDB(&config.Cfg().DB)
	if err != nil {
		logger.Fatalf("new db failed, err: %s", err.Error())
	}

	db2, err := db2.NewDB(&config.Cfg().DB)
	if err != nil {
		logger.Fatalf("new db2 failed, err: %s", err.Error())
	}

	coorSvr, err := coormq.NewServer(mq.NewService(db, db2), &config.Cfg().RabbitMQ)
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

func serveCoorServer(server *coormq.Server, cfg stgmq.Config) {
	logger.Info("start serving command server")

	ch := server.Start(cfg)
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
			break
		}
	}
	logger.Info("command server stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}
