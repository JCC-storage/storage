package models

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/gorm"
	"time"
)

type HubRequest struct {
	RequestID        int64        `gorm:"column:HubID; primaryKey; type:bigint; autoIncrement" json:"hubID"`
	SourceHubID      cdssdk.HubID `gorm:"column:SourceHubID; type:bigint; not null" json:"sourceHubID"`
	TargetHubID      cdssdk.HubID `gorm:"column:TargetHubID; type:bigint; not null" json:"targetHubID"`
	DataTransfer     int64        `gorm:"column:DataTransfer; type:bigint; not null" json:"dataTransfer"`
	RequestCount     int64        `gorm:"column:RequestCount; type:bigint; not null" json:"requestCount"`
	FailedRequest    int64        `gorm:"column:FailedRequest; type:bigint; not null" json:"failedRequest"`
	AvgTransferCount int64        `gorm:"column:AvgTransferCount; type:bigint; not null" json:"avgTransferCount"`
	MaxTransferCount int64        `gorm:"column:MaxTransferCount; type:bigint; not null" json:"maxTransferCount"`
	MinTransferCount int64        `gorm:"column:MinTransferCount; type:bigint; not null" json:"minTransferCount"`
	StartTimestamp   time.Time    `gorm:"column:StartTimestamp; type:datatime; not null" json:"startTimestamp"`
	EndTimestamp     time.Time    `gorm:"column:EndTimestamp; type:datatime; not null" json:"endTimestamp"`
}

type HubRequestRepository struct {
	repo *GormRepository
}

func NewHubRequestRepository(db *gorm.DB) *HubRequestRepository {
	return &HubRequestRepository{repo: NewGormRepository(db)}
}

func (r *HubRequestRepository) CreateHubRequest(request *HubRequest) error {
	return r.repo.Create(request)
}

func ProcessHubTrans(data stgmod.HubTrans) {
	repo := NewHubRequestRepository(db)

	hubRequest := &HubRequest{
		SourceHubID:      cdssdk.HubID(data.Body.SourceHubID),
		TargetHubID:      cdssdk.HubID(data.Body.TargetHubID),
		DataTransfer:     data.Body.DataTransferCount,
		RequestCount:     data.Body.RequestCount,
		FailedRequest:    data.Body.FailedRequestCount,
		AvgTransferCount: data.Body.AvgTransferCount,
		MaxTransferCount: data.Body.MaxTransferCount,
		MinTransferCount: data.Body.MinTransferCount,
		StartTimestamp:   data.Body.StartTimestamp,
		EndTimestamp:     data.Body.EndTimestamp,
	}

	repo.CreateHubRequest(hubRequest)
}
