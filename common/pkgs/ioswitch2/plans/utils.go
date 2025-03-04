package plans

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
)

func getWorkerInfo(hub cdssdk.Hub) exec.WorkerInfo {
	switch addr := hub.Address.(type) {
	case *cdssdk.HttpAddressInfo:
		return &ioswitch2.HttpHubWorker{Hub: hub}

	case *cdssdk.GRPCAddressInfo:
		return &ioswitch2.AgentWorker{Hub: hub, Address: *addr}

	default:
		return nil
	}
}
