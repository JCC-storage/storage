package types

import "gitlink.org.cn/cloudream/common/pkgs/async"

type StorageEvent interface{}

type StorageEventChan = async.UnboundChannel[StorageEvent]

type StorageComponent interface {
	Start(ch *StorageEventChan)
	Stop()
}
