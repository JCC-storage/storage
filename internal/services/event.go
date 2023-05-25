package services

import (
	"gitlink.org.cn/cloudream/common/pkg/logger"
	scmsg "gitlink.org.cn/cloudream/rabbitmq/message/scanner"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

func (svc *Service) PostEvent(msg *scmsg.PostEvent) {

	evtMsg, err := scevt.MapToMessage(msg.Body.Event)
	if err != nil {
		logger.Warnf("convert map to event message failed, err: %s", err.Error())
		return
	}

	evt, err := event.FromMessage(evtMsg)
	if err != nil {
		logger.Warnf("create event from event message failed, err: %s", err.Error())
		return
	}

	svc.eventExecutor.Post(evt, event.ExecuteOption{
		IsEmergency: msg.Body.IsEmergency,
		DontMerge:   msg.Body.DontMerge,
	})
}
