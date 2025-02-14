package mq

import (
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
)

type Service struct {
	db2    *db2.DB
	evtPub *sysevent.Publisher
}

func NewService(db2 *db2.DB, evtPub *sysevent.Publisher) *Service {
	return &Service{
		db2:    db2,
		evtPub: evtPub,
	}
}
