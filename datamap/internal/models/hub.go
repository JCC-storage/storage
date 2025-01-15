package models

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Hub struct {
	HubID   cdssdk.HubID   `gorm:"column:HubID; primaryKey; type:bigint; autoIncrement" json:"hubID"`
	Name    string         `gorm:"column:Name; type:varchar(255); not null" json:"name"`
	Address datatypes.JSON `gorm:"column:Address; type:json; " json:"address"`
}

func (Hub) TableName() string { return "hub" }

type HubRepository struct {
	repo *GormRepository
}

func NewHubRepository(db *gorm.DB) *HubRepository {
	return &HubRepository{repo: NewGormRepository(db)}
}

func (r *HubRepository) CreateHub(hub *Hub) error {
	return r.repo.Create(hub)
}

func (r *HubRepository) UpdateHub(hub *Hub) error {
	return r.repo.Update(hub)
}

func (r *HubRepository) DeleteHub(hub *Hub) error {
	return r.repo.Delete(hub, uint(hub.HubID))
}

func (r *HubRepository) GetHubByID(id int) (*Hub, error) {
	var hub Hub
	err := r.repo.GetByID(uint(id), &hub)
	if err != nil {
		return nil, err
	}
	return &hub, nil
}

func (r *HubRepository) GetAllHubs() ([]Hub, error) {
	var hubs []Hub
	err := r.repo.GetAll(&hubs)
	if err != nil {
		return nil, err
	}
	return hubs, nil
}
