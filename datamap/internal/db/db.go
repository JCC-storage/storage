package db

import (
	"fmt"
	"gitlink.org.cn/cloudream/storage/datamap/internal/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func InitDB(cfg config.DatabaseConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 禁用自动复数化表名
		},
	})
	if err != nil {
		return nil, err
	}

	//// 自动迁移表结构
	//db.AutoMigrate(
	//	&models.Hub{},
	//	&models.Storage{},
	//	&models.HubRequest{},
	//	&models.BlockDistribution{},
	//)

	return db, nil
}
