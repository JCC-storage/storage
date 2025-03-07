package main

import (
	"fmt"
	"os"
	"time"

	_ "google.golang.org/grpc/balancer/grpclb"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/client/internal/cmdline"
	"gitlink.org.cn/cloudream/storage/client/internal/config"
	"gitlink.org.cn/cloudream/storage/client/internal/services"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/accessstat"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader/strategy"
	"gitlink.org.cn/cloudream/storage/common/pkgs/metacache"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
	"gitlink.org.cn/cloudream/storage/common/pkgs/uploader"
)

func main() {
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
	stgglb.InitMQPool(config.Cfg().RabbitMQ)
	stgglb.InitAgentRPCPool(&config.Cfg().AgentGRPC)

	// 连接性信息收集
	var conCol connectivity.Collector
	if config.Cfg().Local.HubID != nil {
		//如果client与某个hub处于同一台机器，则使用这个hub的连通性信息
		coorCli, err := stgglb.CoordinatorMQPool.Acquire()
		if err != nil {
			logger.Warnf("acquire coordinator mq failed, err: %s", err.Error())
			os.Exit(1)
		}
		getCons, err := coorCli.GetHubConnectivities(coormq.ReqGetHubConnectivities([]cdssdk.HubID{*config.Cfg().Local.HubID}))
		if err != nil {
			logger.Warnf("get hub connectivities failed, err: %s", err.Error())
			os.Exit(1)
		}
		consMap := make(map[cdssdk.HubID]connectivity.Connectivity)
		for _, con := range getCons.Connectivities {
			var delay *time.Duration
			if con.Latency != nil {
				d := time.Duration(*con.Latency * float32(time.Millisecond))
				delay = &d
			}
			consMap[con.FromHubID] = connectivity.Connectivity{
				ToHubID: con.ToHubID,
				Latency: delay,
			}
		}
		conCol = connectivity.NewCollectorWithInitData(&config.Cfg().Connectivity, nil, consMap)
		logger.Info("use local hub connectivities")

	} else {
		// 否则需要就地收集连通性信息
		conCol = connectivity.NewCollector(&config.Cfg().Connectivity, nil)
		conCol.CollectInPlace()
	}

	metaCacheHost := metacache.NewHost()
	go metaCacheHost.Serve()
	stgMeta := metaCacheHost.AddStorageMeta()
	hubMeta := metaCacheHost.AddHubMeta()
	conMeta := metaCacheHost.AddConnectivity()

	// 分布式锁
	distlockSvc, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}
	go serveDistLock(distlockSvc)

	// 访问统计
	acStat := accessstat.NewAccessStat(accessstat.Config{
		// TODO 考虑放到配置里
		ReportInterval: time.Second * 10,
	})
	go serveAccessStat(acStat)

	// 存储管理器
	stgAgts := agtpool.NewPool()

	// 任务管理器
	taskMgr := task.NewManager(distlockSvc, &conCol, stgAgts)

	strgSel := strategy.NewSelector(config.Cfg().DownloadStrategy, stgMeta, hubMeta, conMeta)

	// 下载器
	dlder := downloader.NewDownloader(config.Cfg().Downloader, &conCol, stgAgts, strgSel)

	// 上传器
	uploader := uploader.NewUploader(distlockSvc, &conCol, stgAgts, stgMeta)

	svc, err := services.NewService(distlockSvc, &taskMgr, &dlder, acStat, uploader, strgSel, stgMeta)
	if err != nil {
		logger.Warnf("new services failed, err: %s", err.Error())
		os.Exit(1)
	}

	cmds, err := cmdline.NewCommandline(svc)
	if err != nil {
		logger.Warnf("new command line failed, err: %s", err.Error())
		os.Exit(1)
	}

	cmds.DispatchCommand(os.Args[1:])
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
