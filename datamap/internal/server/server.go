package server

import (
	"github.com/gin-gonic/gin"
	"github.com/streadway/amqp"
	"gitlink.org.cn/cloudream/storage/datamap/internal/handlers"
	"gorm.io/gorm"
	"log"
)

func StartServer(db *gorm.DB, mq *amqp.Connection) {
	r := gin.Default()

	// 注册HTTP接口
	r.GET("/storage", handlers.GetStorageData)
	r.GET("/block-distribution", handlers.GetBlockDistributionData)

	// 启动服务
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
