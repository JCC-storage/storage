package models

import (
	"encoding/json"
	"fmt"
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

	if event.Category == "hubInfo" {
		if hubInfo, ok := event.Body.(*stgmod.BodyHubInfo); ok {

			hub := &Hub{
				HubID:   hubInfo.HubID,
				Name:    hubInfo.HubInfo.Name,
				Address: hubInfo.HubInfo.Address,
			}
			//先判断传输数据的类型
			switch hubInfo.Type {
			case "add":
				err := repo.CreateHub(hub)
				if err != nil {
					return
				}
			case "update":
				err := repo.UpdateHub(hub)
				if err != nil {
					return
				}
			case "delete":
				err := repo.DeleteHub(hub)
				if err != nil {
					return
				}
			default:
				return
			}
		} else {
			// 如果 Body 不是我们期望的类型，打印错误信息
			fmt.Printf("Watcher %s: Unexpected Body type, expected *BodyHubInfo, got %T\n", w.Name, event.Body)
		}
	} else {
		// 如果事件的 Category 不是 hubInfo，打印默认信息
		fmt.Printf("Watcher %s received an event with category %s\n", w.Name, event.Category)
	}
}
