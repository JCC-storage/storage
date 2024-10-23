package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	"gitlink.org.cn/cloudream/storage/common/pkgs/grpc"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Config struct {
	ID           int64                      `json:"id"`
	ListenAddr   string                     `json:"listenAddr"`
	Local        stgmodels.LocalMachineInfo `json:"local"`
	GRPC         *grpc.Config               `json:"grpc"`
	Logger       log.Config                 `json:"logger"`
	RabbitMQ     stgmq.Config               `json:"rabbitMQ"`
	DistLock     distlock.Config            `json:"distlock"`
	Connectivity connectivity.Config        `json:"connectivity"`
	Downloader   downloader.Config          `json:"downloader"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("agent", &cfg)
}

func Cfg() *Config {
	return &cfg
}
