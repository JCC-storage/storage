package models

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/gorm"
	"log"
	"time"
)

type HubRequest struct {
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

// ProcessHubTransfer mq推送各节点统计自身当天向外部各个节点传输的总数据量时的处理逻辑
func ProcessHubTransfer(data stgmod.HubTransferStats) {
	repo := NewHubRequestRepository(DB)

	hubRequest := &HubRequest{
		SourceType:         "hub",
		SourceID:           cdssdk.HubID(data.Body.SourceHubID),
		TargetType:         "hub",
		TargetID:           cdssdk.HubID(data.Body.TargetHubID),
		DataTransferCount:  data.Body.Send.TotalTransfer,
		RequestCount:       data.Body.Send.RequestCount,
		FailedRequestCount: data.Body.Send.FailedRequestCount,
		AvgTransferCount:   data.Body.Send.AvgTransfer,
		MaxTransferCount:   data.Body.Send.MaxTransfer,
		MinTransferCount:   data.Body.Send.MinTransfer,
		StartTimestamp:     data.Body.StartTimestamp,
		EndTimestamp:       data.Body.EndTimestamp,
	}

	err := repo.CreateHubRequest(hubRequest)
	if err != nil {
		log.Printf("Error update hubrequest: %v", err)

	}
}

// ProcessHubStorageTransfer  节点中心之间数据传输处理
func ProcessHubStorageTransfer(data stgmod.HubStorageTransferStats) {
	repo := NewHubRequestRepository(DB)

	hubRequestSend := &HubRequest{
		SourceType:         "hub",
		SourceID:           cdssdk.HubID(data.Body.HubID),
		TargetType:         "storage",
		TargetID:           cdssdk.HubID(data.Body.StorageID),
		DataTransferCount:  data.Body.Send.TotalTransfer,
		RequestCount:       data.Body.Send.RequestCount,
		FailedRequestCount: data.Body.Send.FailedRequestCount,
		AvgTransferCount:   data.Body.Send.AvgTransfer,
		MaxTransferCount:   data.Body.Send.MaxTransfer,
		MinTransferCount:   data.Body.Send.MinTransfer,
		StartTimestamp:     data.Body.StartTimestamp,
		EndTimestamp:       data.Body.EndTimestamp,
	}

	err := repo.CreateHubRequest(hubRequestSend)
	if err != nil {
		log.Printf("Error update hubrequest: %v", err)

	}

	hubRequestReceive := &HubRequest{
		SourceType:         "storage",
		SourceID:           cdssdk.HubID(data.Body.StorageID),
		TargetType:         "hub",
		TargetID:           cdssdk.HubID(data.Body.HubID),
		DataTransferCount:  data.Body.Receive.TotalTransfer,
		RequestCount:       data.Body.Receive.RequestCount,
		FailedRequestCount: data.Body.Receive.FailedRequestCount,
		AvgTransferCount:   data.Body.Receive.AvgTransfer,
		MaxTransferCount:   data.Body.Receive.MaxTransfer,
		MinTransferCount:   data.Body.Receive.MinTransfer,
		StartTimestamp:     data.Body.StartTimestamp,
		EndTimestamp:       data.Body.EndTimestamp,
	}

	err = repo.CreateHubRequest(hubRequestReceive)
	if err != nil {
		log.Printf("Error update hubrequest: %v", err)

	}
}
