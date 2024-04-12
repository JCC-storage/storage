package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/agent/internal/config"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"

	// TODO 注册OpUnion，但在mq包中注册会造成循环依赖，所以只能放到这里
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"

	"google.golang.org/grpc"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"

	grpcsvc "gitlink.org.cn/cloudream/storage/agent/internal/grpc"
	cmdsvc "gitlink.org.cn/cloudream/storage/agent/internal/mq"
)

// TODO 此数据是否在运行时会发生变化？
var AgentIpList []string

// 主程序入口
func main() {
	// TODO: 将Agent的IP列表放到配置文件中读取
	AgentIpList = []string{"pcm01", "pcm1", "pcm2"}

	// 初始化配置
	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	// 初始化日志系统
	err = log.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	// 初始化全局变量和连接池
	stgglb.InitLocal(&config.Cfg().Local)
	stgglb.InitMQPool(&config.Cfg().RabbitMQ)
	stgglb.InitAgentRPCPool(&agtrpc.PoolConfig{})
	stgglb.InitIPFSPool(&config.Cfg().IPFS)

	// 启动网络连通性检测，并进行一次就地检测
	conCol := connectivity.NewCollector(&config.Cfg().Connectivity, func(collector *connectivity.Collector) {
		log := log.WithField("Connectivity", "")

		// 从协调器MQ连接池获取客户端
		coorCli, err := stgglb.CoordinatorMQPool.Acquire()
		if err != nil {
			log.Warnf("acquire coordinator mq failed, err: %s", err.Error())
			return
		}

		// 确保在函数返回前释放客户端
		defer stgglb.CoordinatorMQPool.Release(coorCli)

		// 处理网络连通性数据，并更新到协调器
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

	// 初始化分布式锁服务
	distlock, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		log.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	// 初始化数据切换开关
	sw := ioswitch.NewSwitch()

	dlder := downloader.NewDownloader(config.Cfg().Downloader)

	//处置协调端、客户端命令（可多建几个）
	wg := sync.WaitGroup{}
	wg.Add(4)

	taskMgr := task.NewManager(distlock, &sw, &conCol, &dlder)

	// 启动命令服务器
	agtSvr, err := agtmq.NewServer(cmdsvc.NewService(&taskMgr, &sw), config.Cfg().ID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError(func(err error) {
		log.Warnf("agent server err: %s", err.Error())
	})

	go serveAgentServer(agtSvr, &wg)

	// 启动面向客户端的GRPC服务
	listenAddr := config.Cfg().GRPC.MakeListenAddress()
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen on %s failed, err: %s", listenAddr, err.Error())
	}

	s := grpc.NewServer()
	agtrpc.RegisterAgentServer(s, grpcsvc.NewService(&sw))
	go serveGRPC(s, lis, &wg)

	// 启动分布式锁服务的处理程序
	go serveDistLock(distlock)

	// 等待所有服务结束
	wg.Wait()
}

// serveAgentServer 启动并服务一个命令服务器
// server: 指向agtmq.Server的指针，代表要被服务的命令服务器
// wg: 指向sync.WaitGroup的指针，用于等待服务器停止
func serveAgentServer(server *agtmq.Server, wg *sync.WaitGroup) {
	log.Info("start serving command server")

	err := server.Serve()

	if err != nil {
		log.Errorf("command server stopped with error: %s", err.Error())
	}

	log.Info("command server stopped")

	wg.Done() // 表示服务器已经停止
}

// serveGRPC 启动并服务一个gRPC服务器
// s: 指向grpc.Server的指针，代表要被服务的gRPC服务器
// lis: 网络监听器，用于监听gRPC请求
// wg: 指向sync.WaitGroup的指针，用于等待服务器停止
func serveGRPC(s *grpc.Server, lis net.Listener, wg *sync.WaitGroup) {
	log.Info("start serving grpc")

	err := s.Serve(lis)

	if err != nil {
		log.Errorf("grpc stopped with error: %s", err.Error())
	}

	log.Info("grpc stopped")

	wg.Done() // 表示gRPC服务器已经停止
}

// serveDistLock 启动并服务一个分布式锁服务
// svc: 指向distlock.Service的指针，代表要被服务的分布式锁服务
func serveDistLock(svc *distlock.Service) {
	log.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		log.Errorf("distlock stopped with error: %s", err.Error())
	}

	log.Info("distlock stopped")
}
