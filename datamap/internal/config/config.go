package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	db "gitlink.org.cn/cloudream/storage/common/pkgs/db2/config"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Config struct {
	Logger   log.Config      `json:"logger"`
	DB       db.Config       `json:"db"`
	RabbitMQ stgmq.Config    `json:"rabbitMQ"`
	DistLock distlock.Config `json:"distlock"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("datamap", &cfg)
}

func Cfg() *Config {
	return &cfg
}
