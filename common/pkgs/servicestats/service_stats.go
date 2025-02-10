package servicestats

import (
	"sync"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type StatsHost struct {
	// 统计Hub间的传输数据，仅包含当前Hub主动发送或接收的数量。
	HubTransfer *HubTransferStats
	// 统计Hub与存储系统间的传输数据，仅包含当前Hub主动发送或接收的数量。
	HubStorageTransfer *HubStorageTransferStats
}

func (h *StatsHost) SetupHubTransfer(fromHubID cdssdk.HubID) {
	h.HubTransfer = &HubTransferStats{
		fromHubID: fromHubID,
		lock:      &sync.Mutex{},
		data: HubTransferStatsData{
			StartTime: time.Now(),
			Entries:   make(map[cdssdk.HubID]*HubTransferStatsEntry),
		},
	}
}

func (h *StatsHost) SetupHubStorageTransfer(fromHubID cdssdk.HubID) {
	h.HubStorageTransfer = &HubStorageTransferStats{
		fromHubID: fromHubID,
		lock:      &sync.Mutex{},
		data: HubStorageTransferStatsData{
			StartTime: time.Now(),
			Entries:   make(map[cdssdk.StorageID]*HubStorageTransferStatsEntry),
		},
	}
}
