package config

import (
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	db "gitlink.org.cn/cloudream/storage/common/pkgs/db2/config"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Config struct {
	Logger   log.Config   `json:"logger"`
	DB       db.Config    `json:"db"`
	RabbitMQ stgmq.Config `json:"rabbitMQ"`
}

var cfg Config

func Init(path string) error {
	if path == "" {
		return c.DefaultLoad("coordinator", &cfg)
	}

	return c.Load(path, &cfg)
}

func Cfg() *Config {
	return &cfg
}
