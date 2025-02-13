package tickevent

import (
	"gitlink.org.cn/cloudream/common/utils/math2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/agtpool"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
)

func ReportHubTransferStats(evtPub *sysevent.Publisher) {
	if stgglb.Stats.HubTransfer == nil {
		return
	}

	data := stgglb.Stats.HubTransfer.DumpData()
	endTime := stgglb.Stats.HubTransfer.Reset()

	for hubID, entry := range data.Entries {
		evtPub.Publish(&stgmod.BodyHubTransferStats{
			SourceHubID: *stgglb.Local.HubID,
			TargetHubID: hubID,
			Send: stgmod.DataTrans{
				TotalTransfer:      entry.OutputBytes,
				RequestCount:       entry.TotalOutput,
				FailedRequestCount: entry.TotalInput - entry.SuccessInput,
				AvgTransfer:        math2.DivOrDefault(entry.OutputBytes, entry.TotalOutput, 0),
				MinTransfer:        entry.MinOutputBytes,
				MaxTransfer:        entry.MaxOutputBytes,
			},
			StartTimestamp: data.StartTime,
			EndTimestamp:   endTime,
		})
	}
}

func ReportHubStorageTransferStats(stgAgts *agtpool.AgentPool, evtPub *sysevent.Publisher) {
	if stgglb.Stats.HubStorageTransfer == nil {
		return
	}

	data := stgglb.Stats.HubStorageTransfer.DumpData()
	endTime := stgglb.Stats.HubStorageTransfer.Reset()

	for storageID, stg := range data.Entries {
		evtPub.Publish(&stgmod.BodyHubStorageTransferStats{
			HubID:     *stgglb.Local.HubID,
			StorageID: storageID,
			Send: stgmod.DataTrans{
				TotalTransfer:      stg.OutputBytes,
				RequestCount:       stg.TotalOutput,
				FailedRequestCount: stg.TotalInput - stg.SuccessInput,
				AvgTransfer:        math2.DivOrDefault(stg.OutputBytes, stg.TotalOutput, 0),
				MinTransfer:        stg.MinOutputBytes,
				MaxTransfer:        stg.MaxOutputBytes,
			},
			StartTimestamp: data.StartTime,
			EndTimestamp:   endTime,
		})
	}
}
