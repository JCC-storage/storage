package servicestats

import (
	"math"
	"sync"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/math2"
)

type HubStorageTransferStats struct {
	data      HubStorageTransferStatsData
	fromHubID cdssdk.HubID
	lock      *sync.Mutex
}

type HubStorageTransferStatsData struct {
	Entries   map[cdssdk.StorageID]*HubStorageTransferStatsEntry
	StartTime time.Time
}

type HubStorageTransferStatsEntry struct {
	DestStorageID cdssdk.StorageID

	OutputBytes    int64
	MaxOutputBytes int64
	MinOutputBytes int64
	TotalOutput    int64
	SuccessOutput  int64

	InputBytes    int64
	MaxInputBytes int64
	MinInputBytes int64
	TotalInput    int64
	SuccessInput  int64
}

func (s *HubStorageTransferStats) RecordUpload(dstStorageID cdssdk.StorageID, transferBytes int64, isSuccess bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	e := s.data.Entries[dstStorageID]
	if e == nil {
		e = &HubStorageTransferStatsEntry{
			DestStorageID:  dstStorageID,
			MinInputBytes:  math.MaxInt64,
			MinOutputBytes: math.MaxInt64,
		}
		s.data.Entries[dstStorageID] = e
	}
	e.OutputBytes += transferBytes
	e.MaxOutputBytes = math2.Max(e.MaxOutputBytes, transferBytes)
	e.MinOutputBytes = math2.Min(e.MinOutputBytes, transferBytes)
	if isSuccess {
		e.SuccessOutput++
	}
	e.TotalOutput++
}

func (s *HubStorageTransferStats) RecordDownload(dstStorageID cdssdk.StorageID, transferBytes int64, isSuccess bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	e := s.data.Entries[dstStorageID]
	if e == nil {
		e = &HubStorageTransferStatsEntry{
			DestStorageID:  dstStorageID,
			MinInputBytes:  math.MaxInt64,
			MinOutputBytes: math.MaxInt64,
		}
		s.data.Entries[dstStorageID] = e
	}
	e.InputBytes += transferBytes
	e.MaxInputBytes = math2.Max(e.MaxInputBytes, transferBytes)
	e.MinInputBytes = math2.Min(e.MinInputBytes, transferBytes)
	if isSuccess {
		e.SuccessInput++
	}
}

func (s *HubStorageTransferStats) Reset() time.Time {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data.Entries = make(map[cdssdk.StorageID]*HubStorageTransferStatsEntry)
	s.data.StartTime = time.Now()
	return s.data.StartTime
}

func (s *HubStorageTransferStats) DumpData() HubStorageTransferStatsData {
	s.lock.Lock()
	defer s.lock.Unlock()

	data := s.data
	data.Entries = make(map[cdssdk.StorageID]*HubStorageTransferStatsEntry)
	for k, v := range s.data.Entries {
		v2 := *v
		data.Entries[k] = &v2
	}

	return data
}
