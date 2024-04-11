package services

import (
	"fmt"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

// ScannerService 是扫描器服务结构体，封装了与扫描器相关的服务功能。
type ScannerService struct {
	*Service
}

// ScannerSvc 返回ScannerService的一个实例，提供扫描器服务。
func (svc *Service) ScannerSvc() *ScannerService {
	return &ScannerService{Service: svc}
}

// PostEvent 执行数据巡查事件
// event: 需要发送的事件对象。
// isEmergency: 是否为紧急事件，影响事件处理的优先级。
// dontMerge: 是否禁止将该事件与其它事件合并处理。
// 返回值: 发送事件过程中遇到的错误。
func (svc *ScannerService) PostEvent(event scevt.Event, isEmergency bool, dontMerge bool) error {
	// 从扫描器消息池中获取客户端实例
	scCli, err := stgglb.ScannerMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new scanner client: %w", err)
	}
	// 确保扫描器客户端在函数返回前被释放
	defer stgglb.ScannerMQPool.Release(scCli)

	// 向扫描器客户端发送事件
	err = scCli.PostEvent(scmq.NewPostEvent(event, isEmergency, dontMerge))
	if err != nil {
		return fmt.Errorf("request to scanner failed, err: %w", err)
	}

	return nil
}
