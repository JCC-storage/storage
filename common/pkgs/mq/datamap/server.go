package datamap

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	mymq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Service interface {
	HubService
	ObjectService
}

type Server struct {
	service   Service
	rabbitSvr mq.RabbitMQServer
}

func NewServer(svc Service, cfg *mymq.Config) (*Server, error) {
	srv := &Server{
		service: svc,
	}

	rabbitSvr, err := mq.NewRabbitMQServer(
		cfg.MakeConnectingURL(),
		mymq.DATAMAP_QUEUE_NAME,
		func(msg *mq.Message) (*mq.Message, error) {
			return msgDispatcher.Handle(srv.service, msg)
		},
		cfg.Param,
	)
	if err != nil {
		return nil, err
	}

	srv.rabbitSvr = *rabbitSvr

	return srv, nil
}

func (s *Server) Stop() {
	s.rabbitSvr.Close()
}

func (s *Server) Start(cfg mymq.Config) *sync2.UnboundChannel[mq.RabbitMQServerEvent] {
	return s.rabbitSvr.Start()
}

func (s *Server) OnError(callback func(error)) {
	s.rabbitSvr.OnError = callback
}

var msgDispatcher mq.MessageDispatcher = mq.NewMessageDispatcher()

// Register 将Service中的一个接口函数作为指定类型消息的处理函数，同时会注册请求和响应的消息类型
// TODO 需要约束：Service实现了TSvc接口
func Register[TReq mq.MessageBody, TResp mq.MessageBody](svcFn func(svc Service, msg TReq) (TResp, *mq.CodeMessage)) any {
	mq.AddServiceFn(&msgDispatcher, svcFn)
	mq.RegisterMessage[TReq]()
	mq.RegisterMessage[TResp]()

	return nil
}

// RegisterNoReply 将Service中的一个*没有返回值的*接口函数作为指定类型消息的处理函数，同时会注册请求和响应的消息类型
// TODO 需要约束：Service实现了TSvc接口
func RegisterNoReply[TReq mq.MessageBody](svcFn func(svc Service, msg TReq)) any {
	mq.AddNoRespServiceFn(&msgDispatcher, svcFn)
	mq.RegisterMessage[TReq]()

	return nil
}
