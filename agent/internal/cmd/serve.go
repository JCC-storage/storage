package cmd

import (
	"fmt"
	"net"
	"os"
	"time"

	"gitlink.org.cn/cloudream/storage/agent/internal/http"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/agent/internal/config"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/accessstat"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader/strategy"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/metacache"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/svcmgr"
	"gitlink.org.cn/cloudream/storage/common/pkgs/uploader"

	"google.golang.org/grpc"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"

	grpcsvc "gitlink.org.cn/cloudream/storage/agent/internal/grpc"
	cmdsvc "gitlink.org.cn/cloudream/storage/agent/internal/mq"
)

func serve(configPath string) {
	err := config.Init(configPath)
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
	stgglb.InitMQPool(config.Cfg().RabbitMQ)
	stgglb.InitAgentRPCPool(&agtrpc.PoolConfig{})

	// 获取Hub配置
	hubCfg := downloadHubConfig()

	// 初始化存储服务管理器
	stgAgts := svcmgr.NewPool()
	for _, stg := range hubCfg.Storages {
		err := stgAgts.SetupAgent(stg)
		if err != nil {
			fmt.Printf("init storage %v: %v", stg.Storage.String(), err)
			os.Exit(1)
		}
	}

	// 初始化执行器
	worker := exec.NewWorker()

	// 初始化HTTP服务
	httpSvr, err := http.NewServer(config.Cfg().ListenAddr, http.NewService(&worker, stgAgts))
	if err != nil {
		logger.Fatalf("new http server failed, err: %s", err.Error())
	}
	go serveHTTP(httpSvr)

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
		hubCons := make([]cdssdk.HubConnectivity, 0, len(cons))
		for _, con := range cons {
			var delay *float32
			if con.Latency != nil {
				v := float32(con.Latency.Microseconds()) / 1000
				delay = &v
			}

			hubCons = append(hubCons, cdssdk.HubConnectivity{
				FromHubID: *stgglb.Local.HubID,
				ToHubID:   con.ToHubID,
				Latency:   delay,
				TestTime:  con.TestTime,
			})
		}

		_, err = coorCli.UpdateHubConnectivities(coormq.ReqUpdateHubConnectivities(hubCons))
		if err != nil {
			log.Warnf("update hub connectivities: %v", err)
		}
	})
	conCol.CollectInPlace()

	// 初始化元数据缓存服务
	metacacheHost := metacache.NewHost()
	go metacacheHost.Serve()
	stgMeta := metacacheHost.AddStorageMeta()
	hubMeta := metacacheHost.AddHubMeta()
	conMeta := metacacheHost.AddConnectivity()

	// 启动访问统计服务
	acStat := accessstat.NewAccessStat(accessstat.Config{
		// TODO 考虑放到配置里
		ReportInterval: time.Second * 10,
	})
	go serveAccessStat(acStat)

	// 初始化分布式锁服务
	distlock, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	// 初始化下载策略选择器
	strgSel := strategy.NewSelector(config.Cfg().DownloadStrategy, stgMeta, hubMeta, conMeta)

	// 初始化下载器
	dlder := downloader.NewDownloader(config.Cfg().Downloader, &conCol, stgAgts, strgSel)

	// 初始化上传器
	uploader := uploader.NewUploader(distlock, &conCol, stgAgts, stgMeta)

	// 初始化任务管理器
	taskMgr := task.NewManager(distlock, &conCol, &dlder, acStat, stgAgts, uploader)

	// 启动命令服务器
	// TODO 需要设计AgentID持久化机制
	agtSvr, err := agtmq.NewServer(cmdsvc.NewService(&taskMgr, stgAgts), config.Cfg().ID, config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError(func(err error) {
		logger.Warnf("agent server err: %s", err.Error())
	})
	go serveAgentServer(agtSvr)

	// 启动GRPC服务
	listenAddr := config.Cfg().GRPC.MakeListenAddress()
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logger.Fatalf("listen on %s failed, err: %s", listenAddr, err.Error())
	}
	s := grpc.NewServer()
	agtrpc.RegisterAgentServer(s, grpcsvc.NewService(&worker, stgAgts))
	go serveGRPC(s, lis)

	go serveDistLock(distlock)

	foever := make(chan struct{})
	<-foever
}

func downloadHubConfig() coormq.GetHubConfigResp {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		logger.Errorf("new coordinator client: %v", err)
		os.Exit(1)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	cfgResp, err := coorCli.GetHubConfig(coormq.ReqGetHubConfig(cdssdk.HubID(config.Cfg().ID)))
	if err != nil {
		logger.Errorf("getting hub config: %v", err)
		os.Exit(1)
	}

	return *cfgResp
}

func serveAgentServer(server *agtmq.Server) {
	logger.Info("start serving command server")

	ch := server.Start()
loop:
	for {
		val, err := ch.Receive()
		if err != nil {
			logger.Errorf("command server stopped with error: %s", err.Error())
			break
		}

		switch val := val.(type) {
		case error:
			logger.Errorf("rabbitmq connect with error: %v", val)
		case mq.ServerExit:
			if val.Error != nil {
				logger.Errorf("rabbitmq server exit with error: %v", val.Error)
			} else {
				logger.Info("rabbitmq server exit")
			}
			break loop
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

func serveHTTP(server *http.Server) {
	logger.Info("start serving http")

	err := server.Serve()

	if err != nil {
		logger.Errorf("http stopped with error: %s", err.Error())
	}

	logger.Info("http stopped")

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
