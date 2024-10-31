package event

import (
	"fmt"
	"reflect"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	event "gitlink.org.cn/cloudream/common/pkgs/event"
	"gitlink.org.cn/cloudream/common/pkgs/typedispatcher"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type ExecuteArgs struct {
	DB       *db2.DB
	DistLock *distlock.Service
}

type Executor = event.Executor[ExecuteArgs]

type ExecuteContext = event.ExecuteContext[ExecuteArgs]

type Event = event.Event[ExecuteArgs]

type ExecuteOption = event.ExecuteOption

func NewExecutor(db *db2.DB, distLock *distlock.Service) Executor {
	return event.NewExecutor(ExecuteArgs{
		DB:       db,
		DistLock: distLock,
	})
}

var msgDispatcher = typedispatcher.NewTypeDispatcher[Event]()

func FromMessage(msg scevt.Event) (Event, error) {
	event, ok := msgDispatcher.Dispatch(msg)
	if !ok {
		return nil, fmt.Errorf("unknow event message type: %s", reflect.TypeOf(msg).String())
	}

	return event, nil
}

func RegisterMessageConvertor[T any, TEvt Event](converter func(msg T) TEvt) {
	typedispatcher.Add(msgDispatcher, func(msg T) Event {
		return converter(msg)
	})
}
