package mq

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

// GetState 用于获取IPFS节点的状态
// 参数:
// msg: 包含请求信息的GetState消息结构体
// 返回值:
// *agtmq.GetStateResp: 包含响应信息的GetStateResp消息结构体
// *mq.CodeMessage: 错误代码和消息
func (svc *Service) GetState(msg *agtmq.GetState) (*agtmq.GetStateResp, *mq.CodeMessage) {
	var ipfsState string

	// 尝试从IPFS池中获取一个客户端实例
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		// 如果获取失败，记录警告信息，并设置IPFS状态为不可用
		logger.Warnf("new ipfs client: %s", err.Error())
		ipfsState = consts.IPFSStateUnavailable

	} else {
		// 如果获取成功，检查IPFS节点是否正常
		if ipfsCli.IsUp() {
			ipfsState = consts.IPFSStateOK
		} else {
			// 如果节点不正常，设置IPFS状态为不可用
			ipfsState = consts.IPFSStateUnavailable
		}
		// 释放IPFS客户端实例
		ipfsCli.Close()
	}

	// 构造并返回响应
	return mq.ReplyOK(agtmq.NewGetStateResp(ipfsState))
}
