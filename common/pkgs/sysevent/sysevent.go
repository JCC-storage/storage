package sysevent

import (
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

const (
	SysEventQueueName = "SysEventQueue"
)

type SysEvent = stgmod.SysEvent

type Source = stgmod.SysEventSource
