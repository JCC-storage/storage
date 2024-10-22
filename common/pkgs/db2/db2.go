package db2

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type DB struct {
	db *gorm.DB
}

func NewDB(cfg *config.Config) (*DB, error) {
	mydb, err := gorm.Open(mysql.Open(cfg.MakeSourceString()), &gorm.Config{})
	if err != nil {
		logrus.Fatalf("failed to connect to database: %v", err)
	}

	return &DB{
		db: mydb,
	}, nil
}
