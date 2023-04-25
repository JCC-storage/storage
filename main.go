package main

import (
	"fmt"
	"os"

	_ "google.golang.org/grpc/balancer/grpclb"

	"gitlink.org.cn/cloudream/client/internal/cmdline"
	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/client/internal/services"
	racli "gitlink.org.cn/cloudream/rabbitmq/client"
)

func main() {
	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		fmt.Printf("new coordinator client failed, err: %s", err.Error())
		os.Exit(1)
	}

	svc, err := services.NewService(coorClient)
	if err != nil {
		fmt.Printf("new services failed, err: %s", err.Error())
		os.Exit(1)
	}

	cmds, err := cmdline.NewCommandline(svc)
	if err != nil {
		fmt.Printf("new command line failed, err: %s", err.Error())
		os.Exit(1)
	}

	args := os.Args
	cmds.DispatchCommand(args[1], args[2:])
	/*
		TO DO future:
		1. ls命令，显示用户指定桶下的所有对象，及相关的元数据
		2. rm命令，用户指定bucket和object名，执行删除操作
		3. update命令，用户发起对象更新命令，查询元数据，判断对象的冗余方式，删除旧对象（unpin所有的副本或编码块），写入新对象
		4. ipfsStat命令，查看本地有无ipfsdaemon，ipfs目录的使用率
		5. ipfsFlush命令，unpin本地ipfs目录中的所有cid(block)
		6. 改为交互式client，输入用户名及秘钥后进入交互界面
		7. 支持纯缓存类型的IPFS节点，数据一律存在后端存储服务中
	*/
}
