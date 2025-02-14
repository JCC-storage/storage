package models

import (
	"encoding/json"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
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

// 实现 Watcher 接口的结构体
type HubInfoWatcher struct {
	Name string
}

// 实现 OnEvent 方法
func (w *HubInfoWatcher) OnEvent(event sysevent.SysEvent) {

	repo := NewHubRepository(DB)

	switch body := event.Body.(type) {
	case *stgmod.BodyNewHub:
		err := repo.CreateHub(&Hub{
			HubID:   body.Info.HubID,
			Name:    body.Info.Name,
			Address: body.Info.Address,
		})
		if err != nil {
			return
		}

	case *stgmod.BodyHubUpdated:
		err := repo.UpdateHub(&Hub{
			HubID:   body.Info.HubID,
			Name:    body.Info.Name,
			Address: body.Info.Address,
		})
		if err != nil {
			return
		}

	case *stgmod.BodyHubDeleted:
		err := repo.DeleteHub(&Hub{
			HubID: body.HubID,
		})
		if err != nil {
			return
		}
	}
}
