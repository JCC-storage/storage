package db

import (
	"fmt"
	"gitlink.org.cn/cloudream/storage/datamap/internal/config"
	"gitlink.org.cn/cloudream/storage/datamap/internal/models"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 全局数据库连接实例
var DB *gorm.DB

func InitDB(cfg config.DatabaseConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// 自动迁移表结构
	db.AutoMigrate(
		&models.Hub{},
		&models.Storage{},
		&models.HubRequest{},
		&models.BlockDistribution{},
	)

	return db, nil
}
