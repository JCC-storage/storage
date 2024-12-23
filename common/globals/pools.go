package stgglb

import (
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
)

var AgentMQPool agtmq.Pool

var CoordinatorMQPool coormq.Pool

var ScannerMQPool scmq.Pool

// InitMQPool
//
//	@Description: 初始化MQ连接池
//	@param cfg
func InitMQPool(cfg *stgmq.Config) {
	AgentMQPool = agtmq.NewPool(cfg)

	CoordinatorMQPool = coormq.NewPool(cfg)

	ScannerMQPool = scmq.NewPool(cfg)

}

var AgentRPCPool *agtrpc.Pool

// InitAgentRPCPool
//
//	@Description: 初始化AgentRPC连接池
//	@param cfg
func InitAgentRPCPool(cfg *agtrpc.PoolConfig) {
	AgentRPCPool = agtrpc.NewPool(cfg)
}
