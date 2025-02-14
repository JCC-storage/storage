package sysevent

import "gitlink.org.cn/cloudream/common/pkgs/mq"

type Config struct {
	Address  string `json:"address"`
	Account  string `json:"account"`
	Password string `json:"password"`
	VHost    string `json:"vhost"`
}

func ConfigFromMQConfig(mqCfg mq.Config) Config {
	return Config{
		Address:  mqCfg.Address,
		Account:  mqCfg.Account,
		Password: mqCfg.Password,
		VHost:    mqCfg.VHost,
	}
}
