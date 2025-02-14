package servicestats

import (
	"math"
	"sync"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/math2"
)

type HubTransferStats struct {
	data      HubTransferStatsData
	fromHubID cdssdk.HubID
	lock      *sync.Mutex
}

type HubTransferStatsData struct {
	Entries   map[cdssdk.HubID]*HubTransferStatsEntry
	StartTime time.Time
}

type HubTransferStatsEntry struct {
	DestHubID cdssdk.HubID

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

func (s *HubTransferStats) RecordOutput(dstHubID cdssdk.HubID, transferBytes int64, isSuccess bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	e := s.data.Entries[dstHubID]
	if e == nil {
		e = &HubTransferStatsEntry{
			DestHubID:      dstHubID,
			MinInputBytes:  math.MaxInt64,
			MinOutputBytes: math.MaxInt64,
		}
		s.data.Entries[dstHubID] = e
	}
	e.OutputBytes += transferBytes
	e.MaxOutputBytes = math2.Max(e.MaxOutputBytes, transferBytes)
	e.MinOutputBytes = math2.Min(e.MinOutputBytes, transferBytes)
	if isSuccess {
		e.SuccessOutput++
	}
	e.TotalOutput++
}

func (s *HubTransferStats) RecordInput(dstHubID cdssdk.HubID, transferBytes int64, isSuccess bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	e := s.data.Entries[dstHubID]
	if e == nil {
		e = &HubTransferStatsEntry{
			DestHubID:      dstHubID,
			MinInputBytes:  math.MaxInt64,
			MinOutputBytes: math.MaxInt64,
		}
		s.data.Entries[dstHubID] = e
	}
	e.InputBytes += transferBytes
	e.MaxInputBytes = math2.Max(e.MaxInputBytes, transferBytes)
	e.MinInputBytes = math2.Min(e.MinInputBytes, transferBytes)
	if isSuccess {
		e.SuccessInput++
	}
	e.TotalInput++
}

func (s *HubTransferStats) Reset() time.Time {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data.StartTime = time.Now()
	s.data.Entries = make(map[cdssdk.HubID]*HubTransferStatsEntry)
	return s.data.StartTime
}

func (s *HubTransferStats) DumpData() HubTransferStatsData {
	s.lock.Lock()
	defer s.lock.Unlock()

	data := s.data
	data.Entries = make(map[cdssdk.HubID]*HubTransferStatsEntry)
	for k, v := range s.data.Entries {
		v2 := *v
		data.Entries[k] = &v2
	}
	return data
}
