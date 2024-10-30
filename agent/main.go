package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"gitlink.org.cn/cloudream/storage/agent/internal/http"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/agent/internal/config"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/accessstat"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/shard/pool"

	"google.golang.org/grpc"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"

	grpcsvc "gitlink.org.cn/cloudream/storage/agent/internal/grpc"
	cmdsvc "gitlink.org.cn/cloudream/storage/agent/internal/mq"
)

// TODO 此数据是否在运行时会发生变化？
var AgentIpList []string

func main() {
	// TODO 放到配置里读取
	AgentIpList = []string{"pcm01", "pcm1", "pcm2"}

	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	err = logger.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	stgglb.InitLocal(&config.Cfg().Local)
	stgglb.InitMQPool(&config.Cfg().RabbitMQ)
	stgglb.InitAgentRPCPool(&agtrpc.PoolConfig{})

	sw := exec.NewWorker()

	svc := http.NewService(&sw)
	if err != nil {
		logger.Fatalf("new http service failed, err: %s", err.Error())
	}
	server, err := http.NewServer(config.Cfg().ListenAddr, svc)
	err = server.Serve()
	if err != nil {
		logger.Fatalf("http server stopped with error: %s", err.Error())
	}

	// 启动网络连通性检测，并就地检测一次
	conCol := connectivity.NewCollector(&config.Cfg().Connectivity, func(collector *connectivity.Collector) {
		log := logger.WithField("Connectivity", "")

		coorCli, err := stgglb.CoordinatorMQPool.Acquire()
		if err != nil {
			log.Warnf("acquire coordinator mq failed, err: %s", err.Error())
			return
		}
		defer stgglb.CoordinatorMQPool.Release(coorCli)

		cons := collector.GetAll()
		nodeCons := make([]cdssdk.NodeConnectivity, 0, len(cons))
		for _, con := range cons {
			var delay *float32
			if con.Delay != nil {
				v := float32(con.Delay.Microseconds()) / 1000
				delay = &v
			}

			nodeCons = append(nodeCons, cdssdk.NodeConnectivity{
				FromNodeID: *stgglb.Local.NodeID,
				ToNodeID:   con.ToNodeID,
				Delay:      delay,
				TestTime:   con.TestTime,
			})
		}

		_, err = coorCli.UpdateNodeConnectivities(coormq.ReqUpdateNodeConnectivities(nodeCons))
		if err != nil {
			log.Warnf("update node connectivities: %v", err)
		}
	})
	conCol.CollectInPlace()

	acStat := accessstat.NewAccessStat(accessstat.Config{
		// TODO 考虑放到配置里
		ReportInterval: time.Second * 10,
	})
	go serveAccessStat(acStat)

	// TODO2 根据配置实例化Store并加入到Pool中
	shardStorePool := pool.New()

	distlock, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	dlder := downloader.NewDownloader(config.Cfg().Downloader, &conCol)

	taskMgr := task.NewManager(distlock, &conCol, &dlder, acStat, shardStorePool)

	// 启动命令服务器
	// TODO 需要设计AgentID持久化机制
	agtSvr, err := agtmq.NewServer(cmdsvc.NewService(&taskMgr, shardStorePool), config.Cfg().ID, &config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError(func(err error) {
		logger.Warnf("agent server err: %s", err.Error())
	})
	go serveAgentServer(agtSvr)

	//面向客户端收发数据
	listenAddr := config.Cfg().GRPC.MakeListenAddress()
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logger.Fatalf("listen on %s failed, err: %s", listenAddr, err.Error())
	}
	s := grpc.NewServer()
	agtrpc.RegisterAgentServer(s, grpcsvc.NewService(&sw))
	go serveGRPC(s, lis)

	go serveDistLock(distlock)

	foever := make(chan struct{})
	<-foever
}

func serveAgentServer(server *agtmq.Server) {
	logger.Info("start serving command server")

	ch := server.Start()
	if ch == nil {
		logger.Errorf("RabbitMQ logEvent is nil")
		os.Exit(1)
	}

	for {
		val, err := ch.Receive()
		if err != nil {
			logger.Errorf("command server stopped with error: %s", err.Error())
			break
		}

		switch val := val.(type) {
		case error:
			logger.Errorf("rabbitmq connect with error: %v", val)
		case int:
			if val == 1 {
				break
			}
		}
	}
	logger.Info("command server stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func serveGRPC(s *grpc.Server, lis net.Listener) {
	logger.Info("start serving grpc")

	err := s.Serve(lis)

	if err != nil {
		logger.Errorf("grpc stopped with error: %s", err.Error())
	}

	logger.Info("grpc stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func serveDistLock(svc *distlock.Service) {
	logger.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		logger.Errorf("distlock stopped with error: %s", err.Error())
	}

	logger.Info("distlock stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func serveAccessStat(svc *accessstat.AccessStat) {
	logger.Info("start serving access stat")

	ch := svc.Start()
loop:
	for {
		val, err := ch.Receive()
		if err != nil {
			logger.Errorf("access stat stopped with error: %v", err)
			break
		}

		switch val := val.(type) {
		case error:
			logger.Errorf("access stat stopped with error: %v", val)
			break loop
		}
	}
	logger.Info("access stat stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}
