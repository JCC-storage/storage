package models

import (
	"encoding/json"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/datatypes"
)

// LocalHub 本地结构体，嵌入cdssdk.Hub
type LocalHub struct {
	cdssdk.Hub
}

type ConcreteHubType struct {
	Address string
}

func (c ConcreteHubType) GetStorageType() string {
	return c.Address
}

func (c ConcreteHubType) String() string {
	return c.Address
}

func (s *LocalHub) UnmarshalJSON(data []byte) error {
	// 定义一个临时结构体来解析 JSON
	type Alias LocalHub
	aux := &struct {
		Address string `json:"address"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	s.Address = ConcreteHubType{Address: aux.Address}
	return nil
}

func ProcessHubInfo(data stgmod.HubInfo) {
	repo := NewHubRepository(DB)
	jsonData, _ := json.Marshal(data.Body.HubInfo.Address)
	HubInfo := &Hub{
		HubID:   cdssdk.HubID(data.Body.HubID),
		Name:    data.Body.HubInfo.Name,
		Address: datatypes.JSON(jsonData),
	}

	//先判断传输数据的类型
	switch data.Body.Type {
	case "add":
		err := repo.CreateHub(HubInfo)
		if err != nil {
			return
		}
	case "update":
		err := repo.UpdateHub(HubInfo)
		if err != nil {
			return
		}
	case "delete":
		err := repo.DeleteHub(HubInfo)
		if err != nil {
			return
		}
	default:
		return
	}
}
