package mq

import (
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"
)

type Service struct {
	db2 *db2.DB
}

func NewService(db2 *db2.DB) *Service {
	return &Service{
		db2: db2,
	}
}
