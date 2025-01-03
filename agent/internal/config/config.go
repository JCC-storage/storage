package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	c "gitlink.org.cn/cloudream/common/utils/config"
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader/strategy"
	"gitlink.org.cn/cloudream/storage/common/pkgs/grpc"
)

type Config struct {
	ID               cdssdk.HubID               `json:"id"`
	ListenAddr       string                     `json:"listenAddr"`
	Local            stgmodels.LocalMachineInfo `json:"local"`
	GRPC             *grpc.Config               `json:"grpc"`
	Logger           log.Config                 `json:"logger"`
	RabbitMQ         mq.Config                  `json:"rabbitMQ"`
	DistLock         distlock.Config            `json:"distlock"`
	Connectivity     connectivity.Config        `json:"connectivity"`
	Downloader       downloader.Config          `json:"downloader"`
	DownloadStrategy strategy.Config            `json:"downloadStrategy"`
}

var cfg Config

func Init(path string) error {
	if path == "" {
		return c.DefaultLoad("agent", &cfg)
	}

	return c.Load(path, &cfg)
}

func Cfg() *Config {
	return &cfg
}
