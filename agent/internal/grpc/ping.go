package grpc

import (
	"context"

	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
)

// Ping 是一个RPC方法，用于验证服务的可用性。
//
// 参数:
// context.Context: 传递上下文信息，包括请求的元数据和取消信号。
// *agtrpc.PingReq: 传递的Ping请求数据，当前实现中未使用。
//
// 返回值:
// *agtrpc.PingResp: Ping响应数据，当前实现中始终返回空响应。
// error: 如果处理过程中出现错误，则返回错误信息；否则返回nil。
func (s *Service) Ping(context.Context, *agtrpc.PingReq) (*agtrpc.PingResp, error) {
	return &agtrpc.PingResp{}, nil
}
