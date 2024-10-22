package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"time"
)

type NodeDB struct {
	*DB
}

func (nodeDB *DB) Node() *NodeDB {
	return &NodeDB{DB: nodeDB}
}

func (nodeDB *NodeDB) GetAllNodes() ([]cdssdk.Node, error) {
	var ret []cdssdk.Node

	err := nodeDB.DB.db.Table("node").Find(&ret).Error
	return ret, err
}

func (nodeDB *NodeDB) GetByID(nodeID cdssdk.NodeID) (cdssdk.Node, error) {
	var ret cdssdk.Node
	err := nodeDB.DB.db.Table("node").Where("NodeID = ?", nodeID).Find(&ret).Error

	return ret, err
}

// GetUserNodes 根据用户id查询可用node
func (nodeDB *NodeDB) GetUserNodes(userID cdssdk.UserID) ([]cdssdk.Node, error) {
	var nodes []cdssdk.Node
	err := nodeDB.DB.db.
		Table("Node").
		Select("Node.*").
		Joins("JOIN UserNode ON UserNode.NodeID = Node.NodeID").
		Where("UserNode.UserID = ?", userID).
		Find(&nodes).Error
	return nodes, err
}

// UpdateState 更新状态，并且设置上次上报时间为现在
func (nodeDB *NodeDB) UpdateState(nodeID cdssdk.NodeID, state string) error {
	err := nodeDB.DB.db.
		Model(&cdssdk.Node{}).
		Where("NodeID = ?", nodeID).
		Updates(map[string]interface{}{
			"State":          state,
			"LastReportTime": time.Now(),
		}).Error
	return err
}
