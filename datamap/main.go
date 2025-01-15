package main

import (
	"gitlink.org.cn/cloudream/storage/datamap/internal/config"
	"gitlink.org.cn/cloudream/storage/datamap/internal/db"
	"gitlink.org.cn/cloudream/storage/datamap/internal/models"
	"gitlink.org.cn/cloudream/storage/datamap/internal/mq"
	"gitlink.org.cn/cloudream/storage/datamap/internal/server"
	"log"
)

func main() {
	// 加载配置
	cfg := config.LoadConfig()

	// 初始化数据库
	dbConn, err := db.InitDB(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	models.InitDB(dbConn)

	// 初始化RabbitMQ
	mqConn, err := mq.InitMQ(cfg.RabbitMQ)
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ: %v", err)
	}

	// 启动Gin服务
	server.StartServer(dbConn, mqConn)
}
