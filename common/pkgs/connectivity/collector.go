package connectivity

import (
	"math/rand"
	"sync"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type Connectivity struct {
	ToHubID  cdssdk.HubID
	Delay    *time.Duration
	TestTime time.Time
}

type Collector struct {
	cfg            *Config
	onCollected    func(collector *Collector)
	collectNow     chan any
	close          chan any
	connectivities map[cdssdk.HubID]Connectivity
	lock           *sync.RWMutex
}

func NewCollector(cfg *Config, onCollected func(collector *Collector)) Collector {
	rpt := Collector{
		cfg:            cfg,
		collectNow:     make(chan any),
		close:          make(chan any),
		connectivities: make(map[cdssdk.HubID]Connectivity),
		lock:           &sync.RWMutex{},
		onCollected:    onCollected,
	}
	go rpt.serve()
	return rpt
}

func NewCollectorWithInitData(cfg *Config, onCollected func(collector *Collector), initData map[cdssdk.HubID]Connectivity) Collector {
	rpt := Collector{
		cfg:            cfg,
		collectNow:     make(chan any),
		close:          make(chan any),
		connectivities: initData,
		lock:           &sync.RWMutex{},
		onCollected:    onCollected,
	}
	go rpt.serve()
	return rpt
}

func (r *Collector) Get(hubID cdssdk.HubID) *Connectivity {
	r.lock.RLock()
	defer r.lock.RUnlock()

	con, ok := r.connectivities[hubID]
	if ok {
		return &con
	}

	return nil
}
func (r *Collector) GetAll() map[cdssdk.HubID]Connectivity {
	r.lock.RLock()
	defer r.lock.RUnlock()

	ret := make(map[cdssdk.HubID]Connectivity)
	for k, v := range r.connectivities {
		ret[k] = v
	}

	return ret
}

// 启动一次收集
func (r *Collector) CollecNow() {
	select {
	case r.collectNow <- nil:
	default:
	}
}

// 就地进行收集，会阻塞当前线程
func (r *Collector) CollectInPlace() {
	r.testing()
}

func (r *Collector) Close() {
	select {
	case r.close <- nil:
	default:
	}
}

func (r *Collector) serve() {
	log := logger.WithType[Collector]("")
	log.Info("start connectivity reporter")

	// 为了防止同时启动的节点会集中进行Ping，所以第一次上报间隔为0-TestInterval秒之间随机
	startup := true
	firstReportDelay := time.Duration(float64(r.cfg.TestInterval) * float64(time.Second) * rand.Float64())
	ticker := time.NewTicker(firstReportDelay)

loop:
	for {
		select {
		case <-ticker.C:
			r.testing()
			if startup {
				startup = false
				ticker.Reset(time.Duration(r.cfg.TestInterval) * time.Second)
			}

		case <-r.collectNow:
			r.testing()

		case <-r.close:
			ticker.Stop()
			break loop
		}
	}

	log.Info("stop connectivity reporter")
}

func (r *Collector) testing() {
	log := logger.WithType[Collector]("")
	log.Debug("do testing")

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getHubResp, err := coorCli.GetHubs(coormq.NewGetHubs(nil))
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}
	cons := make([]Connectivity, len(getHubResp.Hubs))
	for i, hub := range getHubResp.Hubs {
		tmpIdx := i
		tmpHub := hub

		wg.Add(1)
		go func() {
			defer wg.Done()
			cons[tmpIdx] = r.ping(tmpHub)
		}()
	}

	wg.Wait()

	r.lock.Lock()
	// 删除所有hub的记录，然后重建，避免hub数量变化时导致残余数据
	r.connectivities = make(map[cdssdk.HubID]Connectivity)
	for _, con := range cons {
		r.connectivities[con.ToHubID] = con
	}
	r.lock.Unlock()

	if r.onCollected != nil {
		r.onCollected(r)
	}
}

func (r *Collector) ping(hub cdssdk.Hub) Connectivity {
	log := logger.WithType[Collector]("").WithField("HubID", hub.HubID)

	var ip string
	var port int
	switch addr := hub.Address.(type) {
	case *cdssdk.GRPCAddressInfo:
		if hub.LocationID == stgglb.Local.LocationID {
			ip = addr.LocalIP
			port = addr.LocalGRPCPort
		} else {
			ip = addr.ExternalIP
			port = addr.ExternalGRPCPort
		}
	default:
		// TODO 增加对HTTP模式的agent的支持

		log.Warnf("unsupported address type: %v", addr)

		return Connectivity{
			ToHubID:  hub.HubID,
			Delay:    nil,
			TestTime: time.Now(),
		}
	}

	agtCli, err := stgglb.AgentRPCPool.Acquire(ip, port)
	if err != nil {
		log.Warnf("new agent %v:%v rpc client: %w", ip, port, err)
		return Connectivity{
			ToHubID:  hub.HubID,
			Delay:    nil,
			TestTime: time.Now(),
		}
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	// 第一次ping保证网络连接建立成功
	err = agtCli.Ping()
	if err != nil {
		log.Warnf("pre ping: %v", err)
		return Connectivity{
			ToHubID:  hub.HubID,
			Delay:    nil,
			TestTime: time.Now(),
		}
	}

	// 后几次ping计算延迟
	var avgDelay time.Duration
	for i := 0; i < 3; i++ {
		start := time.Now()
		err = agtCli.Ping()
		if err != nil {
			log.Warnf("ping: %v", err)
			return Connectivity{
				ToHubID:  hub.HubID,
				Delay:    nil,
				TestTime: time.Now(),
			}
		}

		delay := time.Since(start)
		avgDelay += delay

		// 每次ping之间间隔1秒
		<-time.After(time.Second)
	}
	delay := avgDelay / 3

	return Connectivity{
		ToHubID:  hub.HubID,
		Delay:    &delay,
		TestTime: time.Now(),
	}
}
