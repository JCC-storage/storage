package models

import (
	"fmt"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gorm.io/gorm"
	"log"
	"time"
)

type HubRequest struct {
	//todo source和target类型的区分
	RequestID          int64        `gorm:"column:RequestID; primaryKey; type:bigint; autoIncrement" json:"RequestID"`
	SourceType         string       `gorm:"column:SourceType; type:varchar(255); not null" json:"sourceType"`
	SourceID           cdssdk.HubID `gorm:"column:SourceID; type:bigint; not null" json:"sourceID"`
	TargetType         string       `gorm:"column:TargetType; type:varchar(255); not null" json:"targetType"`
	TargetID           cdssdk.HubID `gorm:"column:TargetID; type:bigint; not null" json:"targetID"`
	DataTransferCount  int64        `gorm:"column:DataTransferCount; type:bigint; not null" json:"dataTransferCount"`
	RequestCount       int64        `gorm:"column:RequestCount; type:bigint; not null" json:"requestCount"`
	FailedRequestCount int64        `gorm:"column:FailedRequestCount; type:bigint; not null" json:"failedRequestCount"`
	AvgTransferCount   int64        `gorm:"column:AvgTransferCount; type:bigint; not null" json:"avgTransferCount"`
	MaxTransferCount   int64        `gorm:"column:MaxTransferCount; type:bigint; not null" json:"maxTransferCount"`
	MinTransferCount   int64        `gorm:"column:MinTransferCount; type:bigint; not null" json:"minTransferCount"`
	StartTimestamp     time.Time    `gorm:"column:StartTimestamp; type:datatime; not null" json:"startTimestamp"`
	EndTimestamp       time.Time    `gorm:"column:EndTimestamp; type:datatime; not null" json:"endTimestamp"`
}

func (HubRequest) TableName() string { return "hubrequest" }

type HubRequestRepository struct {
	repo *GormRepository
}

func NewHubRequestRepository(db *gorm.DB) *HubRequestRepository {
	return &HubRequestRepository{repo: NewGormRepository(db)}
}

func (r *HubRequestRepository) CreateHubRequest(request *HubRequest) error {
	return r.repo.Create(request)
}

func (r *HubRequestRepository) GetHubRequestByHubID(hubId int64) ([]HubRequest, error) {
	var hubRequests []HubRequest
	query := "SELECT * FROM hubrequest WHERE SourceHubID = ?"
	err := r.repo.db.Raw(query, hubId).Scan(&hubRequests).Error
	if err != nil {
		return nil, err
	}
	return hubRequests, nil
}

func (r *HubRequestRepository) GetAllHubRequests() ([]HubRequest, error) {
	var hubRequests []HubRequest
	err := r.repo.GetAll(&hubRequests)
	if err != nil {
		return nil, err
	}
	return hubRequests, nil
}

type HubTransferStatsWatcher struct {
	Name string
}

func (w *HubTransferStatsWatcher) OnEvent(event sysevent.SysEvent) {
	repo := NewHubRequestRepository(DB)

	if event.Category == "hubTransferStats" {
		if hubTransferStats, ok := event.Body.(*stgmod.BodyHubTransferStats); ok {
			hubRequest := &HubRequest{
				SourceType:         "hub",
				SourceID:           hubTransferStats.SourceHubID,
				TargetType:         "hub",
				TargetID:           hubTransferStats.TargetHubID,
				DataTransferCount:  hubTransferStats.Send.TotalTransfer,
				RequestCount:       hubTransferStats.Send.RequestCount,
				FailedRequestCount: hubTransferStats.Send.FailedRequestCount,
				AvgTransferCount:   hubTransferStats.Send.AvgTransfer,
				MaxTransferCount:   hubTransferStats.Send.MaxTransfer,
				MinTransferCount:   hubTransferStats.Send.MinTransfer,
				StartTimestamp:     hubTransferStats.StartTimestamp,
				EndTimestamp:       hubTransferStats.EndTimestamp,
			}

			err := repo.CreateHubRequest(hubRequest)
			if err != nil {
				log.Printf("Error update hubrequest: %v", err)

			}
		} else {
			fmt.Printf("Watcher %s: Unexpected Body type, expected *BodyStorageInfo, got %T\n", w.Name, event.Body)
		}
	} else {
		fmt.Printf("Watcher %s received an event with category %s\n", w.Name, event.Category)
	}
}

type HubStorageTransferStatsWatcher struct {
	Name string
}

func (w *HubStorageTransferStatsWatcher) OnEvent(event sysevent.SysEvent) {
	repo := NewHubRequestRepository(DB)

	if event.Category == "hubStorageTransferStats" {
		if hubStorageTransferStats, ok := event.Body.(*stgmod.BodyHubStorageTransferStats); ok {

			hubRequestSend := &HubRequest{
				SourceType:         "hub",
				SourceID:           hubStorageTransferStats.HubID,
				TargetType:         "storage",
				TargetID:           cdssdk.HubID(hubStorageTransferStats.StorageID),
				DataTransferCount:  hubStorageTransferStats.Send.TotalTransfer,
				RequestCount:       hubStorageTransferStats.Send.RequestCount,
				FailedRequestCount: hubStorageTransferStats.Send.FailedRequestCount,
				AvgTransferCount:   hubStorageTransferStats.Send.AvgTransfer,
				MaxTransferCount:   hubStorageTransferStats.Send.MaxTransfer,
				MinTransferCount:   hubStorageTransferStats.Send.MinTransfer,
				StartTimestamp:     hubStorageTransferStats.StartTimestamp,
				EndTimestamp:       hubStorageTransferStats.EndTimestamp,
			}

			err := repo.CreateHubRequest(hubRequestSend)
			if err != nil {
				log.Printf("Error update hubrequest: %v", err)

			}

			hubRequestReceive := &HubRequest{
				SourceType:         "storage",
				SourceID:           cdssdk.HubID(hubStorageTransferStats.StorageID),
				TargetType:         "hub",
				TargetID:           hubStorageTransferStats.HubID,
				DataTransferCount:  hubStorageTransferStats.Receive.TotalTransfer,
				RequestCount:       hubStorageTransferStats.Receive.RequestCount,
				FailedRequestCount: hubStorageTransferStats.Receive.FailedRequestCount,
				AvgTransferCount:   hubStorageTransferStats.Receive.AvgTransfer,
				MaxTransferCount:   hubStorageTransferStats.Receive.MaxTransfer,
				MinTransferCount:   hubStorageTransferStats.Receive.MinTransfer,
				StartTimestamp:     hubStorageTransferStats.StartTimestamp,
				EndTimestamp:       hubStorageTransferStats.EndTimestamp,
			}

			err = repo.CreateHubRequest(hubRequestReceive)
			if err != nil {
				log.Printf("Error update hubrequest: %v", err)

			}
		} else {
			fmt.Printf("Watcher %s: Unexpected Body type, expected *BodyStorageInfo, got %T\n", w.Name, event.Body)
		}
	} else {
		fmt.Printf("Watcher %s received an event with category %s\n", w.Name, event.Category)
	}
}
