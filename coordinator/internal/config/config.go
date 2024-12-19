package config

import (
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	c "gitlink.org.cn/cloudream/common/utils/config"
	db "gitlink.org.cn/cloudream/storage/common/pkgs/db2/config"
)

type Config struct {
	Logger   log.Config `json:"logger"`
	DB       db.Config  `json:"db"`
	RabbitMQ mq.Config  `json:"rabbitMQ"`
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
