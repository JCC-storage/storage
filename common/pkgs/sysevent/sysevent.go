package sysevent

import (
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"time"
)

const (
	SysEventQueueName = "SysEventQueue"
)

type SysEvent struct {
	Timestamp time.Time     `json:"timestamp"`
	Source    stgmod.Source `json:"source"`
	Category  string        `json:"category"`
	Body      stgmod.Body   `json:"body"`
}
