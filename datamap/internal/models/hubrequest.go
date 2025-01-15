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
	SourceID           cdssdk.HubID `gorm:"column:SourceHubID; type:bigint; not null" json:"sourceID"`
	TargetType         string       `gorm:"column:TargetType; type:varchar(255); not null" json:"targetType"`
	TargetID           cdssdk.HubID `gorm:"column:TargetHubID; type:bigint; not null" json:"targetID"`
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

	//todo 加字段
	hubRequest := &HubRequest{

		SourceID:           cdssdk.HubID(data.Body.SourceHubID),
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
