package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/config"
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Config struct {
	Local        stgmodels.LocalMachineInfo `json:"local"`
	AgentGRPC    agtrpc.PoolConfig          `json:"agentGRPC"`
	Logger       logger.Config              `json:"logger"`
	RabbitMQ     stgmq.Config               `json:"rabbitMQ"`
	DistLock     distlock.Config            `json:"distlock"`
	Connectivity connectivity.Config        `json:"connectivity"`
	Downloader   downloader.Config          `json:"downloader"`
	StorageID    cdssdk.StorageID           `json:"storageID"` // TODO 进行访问量统计时，当前客户端所属的存储ID。临时解决方案。
}

var cfg Config

// Init 初始化client
// TODO 这里的modeulName参数弄成可配置的更好
func Init() error {
	return config.DefaultLoad("client", &cfg)
}

func Cfg() *Config {
	return &cfg
}
