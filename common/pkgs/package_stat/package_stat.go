package packagestat

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

type PackageStatEvent interface{}

type amountKey struct {
	PackageID cdssdk.PackageID
	NodeID    cdssdk.NodeID
}

type amount struct {
	Counter float64
}

type PackageStat struct {
	cfg     Config
	amounts map[amountKey]*amount
	lock    sync.Mutex
}

func NewPackageStat(cfg Config) *PackageStat {
	return &PackageStat{
		cfg:     cfg,
		amounts: make(map[amountKey]*amount),
	}
}

func (p *PackageStat) AddAccessCounter(pkgID cdssdk.PackageID, nodeID cdssdk.NodeID, value float64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	key := amountKey{
		PackageID: pkgID,
		NodeID:    nodeID,
	}
	if _, ok := p.amounts[key]; !ok {
		p.amounts[key] = &amount{}
	}
	p.amounts[key].Counter += value
}

func (p *PackageStat) Start() *sync2.UnboundChannel[PackageStatEvent] {
	ch := sync2.NewUnboundChannel[PackageStatEvent]()

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
			amts := p.amounts
			p.amounts = make(map[amountKey]*amount)

			var addEntries []coormq.AddPackageAccessStatCounterEntry
			for key, amount := range amts {
				addEntries = append(addEntries, coormq.AddPackageAccessStatCounterEntry{
					PackageID: key.PackageID,
					NodeID:    key.NodeID,
					Value:     amount.Counter,
				})
			}
			p.lock.Unlock()

			_, err := coorCli.AddPackageAccessStatCounter(coormq.NewAddPackageAccessStatCounter(addEntries))
			if err != nil {
				logger.Errorf("add all package access stat counter: %v", err)

				p.lock.Lock()
				for key, a := range amts {
					if _, ok := p.amounts[key]; !ok {
						p.amounts[key] = &amount{}
					}
					p.amounts[key].Counter += a.Counter
				}
				p.lock.Unlock()
				continue
			}
		}
	}()
	return ch
}
