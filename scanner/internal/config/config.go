package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	c "gitlink.org.cn/cloudream/common/utils/config"
	db "gitlink.org.cn/cloudream/storage/common/pkgs/db2/config"
)

type Config struct {
	AccessStatHistoryAmount float64         `json:"accessStatHistoryAmount"`
	ECFileSizeThreshold     int64           `json:"ecFileSizeThreshold"`
	HubUnavailableSeconds   int             `json:"hubUnavailableSeconds"` // 如果节点上次上报时间超过这个值，则认为节点已经不可用
	Logger                  log.Config      `json:"logger"`
	DB                      db.Config       `json:"db"`
	RabbitMQ                mq.Config       `json:"rabbitMQ"`
	DistLock                distlock.Config `json:"distlock"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("scanner", &cfg)
}

func Cfg() *Config {
	return &cfg
}
