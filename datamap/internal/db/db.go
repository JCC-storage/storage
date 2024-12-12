package db

import (
	"github.com/sirupsen/logrus"
	"gitlink.org.cn/cloudream/storage/datamap/internal/db/config"
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

func (db *DB) DoTx(do func(tx SQLContext) error) error {
	return db.db.Transaction(func(tx *gorm.DB) error {
		return do(SQLContext{tx})
	})
}

type SQLContext struct {
	*gorm.DB
}

func (db *DB) DefCtx() SQLContext {
	return SQLContext{db.db}
}
