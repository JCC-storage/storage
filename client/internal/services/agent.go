// services 包提供了与代理服务相关的功能。
package services

type AgentService struct {
	*Service // Service 是嵌入的基服务类型，为AgentService提供基本功能。
}

// AgentSvc 是Service类型的一个方法，用于返回一个AgentService的实例。
// 该方法允许通过Service实例来访问或操作AgentService相关功能。
//
// 参数:
//
//	svc *Service - 指向当前Service实例的指针。
//
// 返回值:
//
//	*AgentService - 指向新创建的AgentService实例的指针。
func (svc *Service) AgentSvc() *AgentService {
	return &AgentService{Service: svc}
}
