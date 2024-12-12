package cmd

import (
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/datamap/internal/config"
)

func serve() {
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

	// dataSvr, err := datamq.NewServer(mymq.NewService(db2), &config.Cfg().RabbitMQ)
	// if err != nil {
	// 	logger.Fatalf("new coordinator server failed, err: %s", err.Error())
	// }
}
