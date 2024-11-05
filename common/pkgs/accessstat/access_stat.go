package accessstat

import (
	"fmt"
	"sync"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type AccessStatEvent interface{}

type AccessStat struct {
	cfg   Config
	stats []coormq.AddAccessStatEntry
	lock  sync.Mutex
}

func NewAccessStat(cfg Config) *AccessStat {
	return &AccessStat{
		cfg: cfg,
	}
}

func (p *AccessStat) AddAccessCounter(objID cdssdk.ObjectID, pkgID cdssdk.PackageID, stgID cdssdk.StorageID, value float64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.stats = append(p.stats, coormq.AddAccessStatEntry{
		ObjectID:  objID,
		PackageID: pkgID,
		StorageID: stgID,
		Counter:   value,
	})
}

func (p *AccessStat) Start() *sync2.UnboundChannel[AccessStatEvent] {
	ch := sync2.NewUnboundChannel[AccessStatEvent]()

	go func() {
		coorCli, err := stgglb.CoordinatorMQPool.Acquire()
		if err != nil {
			ch.Send(fmt.Errorf("new coordinator client: %w", err))
		}
		defer stgglb.CoordinatorMQPool.Release(coorCli)

		ticker := time.NewTicker(p.cfg.ReportInterval)
		for {
			<-ticker.C

			p.lock.Lock()
			st := p.stats
			p.stats = nil
			p.lock.Unlock()

			if len(st) == 0 {
				continue
			}

			err := coorCli.AddAccessStat(coormq.ReqAddAccessStat(st))
			if err != nil {
				logger.Errorf("add all package access stat counter: %v", err)

				p.lock.Lock()
				p.stats = append(p.stats, st...)
				p.lock.Unlock()
				continue
			}
		}
	}()
	return ch
}
