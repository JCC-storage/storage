package mq

import (
	mydb "gitlink.org.cn/cloudream/storage/common/pkgs/db"
	mydb2 "gitlink.org.cn/cloudream/storage/common/pkgs/db2"
)

type Service struct {
	db  *mydb.DB
	db2 *mydb2.DB
}

func NewService(db *mydb.DB, db2 *mydb2.DB) *Service {
	return &Service{
		db:  db,
		db2: db2,
	}
}
