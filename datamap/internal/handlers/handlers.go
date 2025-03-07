package handlers

import (
	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/storage/datamap/internal/models"
	"gorm.io/gorm"
	"net/http"
	"strconv"
)

// DB 全局数据库连接实例
var DB *gorm.DB

// SetDB 设置数据库连接实例
func SetDB(db *gorm.DB) {
	DB = db
}

// GetHubInfo 获取 节点交互数据
func GetHubInfo(c *gin.Context) {

	repoHub := models.NewHubRepository(DB)
	repoStorage := models.NewStorageRepository(DB)
	repoHubReq := models.NewHubRequestRepository(DB)

	nodes := make([]models.Node, 0)
	edges := make([]models.Edge, 0)

	//添加所有节点信息
	hubs, _ := repoHub.GetAllHubs()
	storages, _ := repoStorage.GetAllStorages()
	for _, hub := range hubs {
		node := models.Node{
			ID:       "hub" + strconv.FormatInt(int64(hub.HubID), 10),
			NodeType: "hub",
			Name:     hub.Name,
			Address:  hub.Address,
		}
		nodes = append(nodes, node)
	}
	for _, storage := range storages {
		node := models.Node{
			ID:           "storage" + strconv.FormatInt(int64(storage.StorageID), 10),
			NodeType:     "storage",
			Name:         storage.StorageName,
			DataCount:    storage.DataCount,
			NewDataCount: storage.NewDataCount,
			Timestamp:    storage.Timestamp,
		}
		nodes = append(nodes, node)
	}

	// 添加所有边信息
	hubReqs, _ := repoHubReq.GetAllHubRequests()
	for _, hubReq := range hubReqs {
		edge := models.Edge{
			SourceType:         hubReq.SourceType,
			SourceID:           hubReq.SourceType + strconv.FormatInt(int64(hubReq.SourceID), 10),
			TargetType:         hubReq.TargetType,
			TargetID:           hubReq.TargetType + strconv.FormatInt(int64(hubReq.TargetID), 10),
			DataTransferCount:  hubReq.DataTransferCount,
			RequestCount:       hubReq.RequestCount,
			FailedRequestCount: hubReq.FailedRequestCount,
			AvgTransferCount:   hubReq.AvgTransferCount,
			MaxTransferCount:   hubReq.MaxTransferCount,
			MinTransferCount:   hubReq.MinTransferCount,
			StartTimestamp:     hubReq.StartTimestamp,
			EndTimestamp:       hubReq.EndTimestamp,
		}
		edges = append(edges, edge)
	}
	hubRelationship := models.HubRelationship{
		Nodes: nodes,
		Edges: edges,
	}
	c.JSON(http.StatusOK, hubRelationship)
}

func containsCombo(combos []models.Combo, targetID string, targetComboType string) bool {
	for _, combo := range combos {
		if combo.ID == targetID && combo.ComboType == targetComboType {
			return true
		}
	}
	return false
}

// GetDataTransfer 数据对象的节点间传输量
func GetDataTransfer(c *gin.Context) {

	repoObject := models.NewObjectRepository(DB)
	repoBlockDistribution := models.NewBlockDistributionRepository(DB)
	repoStorageTrans := models.NewStorageTransferCountRepository(DB)

	//首先判断object是否存在
	objectIDStr := c.Param("objectID")
	objectID, _ := strconv.ParseInt(objectIDStr, 10, 64)
	object, _ := repoObject.GetObjectByID(objectID)
	if object == nil {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}

	nodes := make([]models.DistNode, 0)
	combos := make([]models.Combo, 0)
	edges := make([]models.DistEdge, 0)

	//根据ObjectID查询出在所有storage中存储的块或副本
	blocks, _ := repoBlockDistribution.GetBlockDistributionByObjectID(objectID)
	for _, block := range blocks {
		//nodes   ---------    block
		//添加node信息
		node := models.DistNode{
			//block id
			ID: strconv.FormatInt(block.BlockID, 10),
			//storage id
			ComboID: "storage" + strconv.FormatInt(block.StorageID, 10),
			//block index
			Label: block.Type + strconv.FormatInt(block.Index, 10),
			//block type
			NodeType: block.Type,
		}
		nodes = append(nodes, node)

		//combos -------    state or storage
		//添加storage combo信息
		if !containsCombo(combos, "storage"+strconv.FormatInt(block.StorageID, 10), "storage") {
			combo := models.Combo{
				ID:        "storage" + strconv.FormatInt(block.StorageID, 10),
				Label:     "存储中心" + strconv.FormatInt(block.StorageID, 10),
				ParentId:  "state" + strconv.Itoa(block.Status),
				ComboType: "storage",
			}
			combos = append(combos, combo)
		}
		//添加state combo信息
		if !containsCombo(combos, "state"+strconv.Itoa(block.Status), "state") {
			var statusStr string
			switch block.Status {
			case 0:
				statusStr = "实时情况"
			case 1:
				statusStr = block.Timestamp.Format("2006-01-02") + "布局调整后"
			case 2:
				statusStr = block.Timestamp.Format("2006-01-02") + "布局调整前"
			case 3:
				statusStr = block.Timestamp.Format("2006-01-02") + "布局调整后"
			default:
				statusStr = "未知状态"
			}
			combo := models.Combo{
				ID:        "state" + strconv.Itoa(block.Status),
				Label:     statusStr,
				ComboType: "state",
			}
			combos = append(combos, combo)
		}
	}
	//edges data trans between storage and storage
	relations, _ := repoStorageTrans.GetStorageTransferCountByObjectID(objectID)
	for _, relation := range relations {
		edge := models.DistEdge{
			Source: "storage" + strconv.FormatInt(relation.SourceStorageID, 10),
			Target: "storage" + strconv.FormatInt(relation.TargetStorageID, 10),
		}
		edges = append(edges, edge)
	}
	result := models.ObjectDistribution{
		Nodes:  nodes,
		Combos: combos,
		Edges:  edges,
	}
	c.JSON(http.StatusOK, result)
}
