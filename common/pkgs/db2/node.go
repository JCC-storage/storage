package db2

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type NodeDB struct {
	*DB
}

func (db *DB) Node() *NodeDB {
	return &NodeDB{DB: db}
}

func (*NodeDB) GetAllNodes(ctx SQLContext) ([]cdssdk.Node, error) {
	var ret []cdssdk.Node

	err := ctx.Table("node").Find(&ret).Error
	return ret, err
}

func (*NodeDB) GetByID(ctx SQLContext, nodeID cdssdk.NodeID) (cdssdk.Node, error) {
	var ret cdssdk.Node
	err := ctx.Table("node").Where("NodeID = ?", nodeID).Find(&ret).Error

	return ret, err
}

// GetUserNodes 根据用户id查询可用node
func (*NodeDB) GetUserNodes(ctx SQLContext, userID cdssdk.UserID) ([]cdssdk.Node, error) {
	var nodes []cdssdk.Node
	err := ctx.
		Table("Node").
		Select("Node.*").
		Joins("JOIN UserNode ON UserNode.NodeID = Node.NodeID").
		Where("UserNode.UserID = ?", userID).
		Find(&nodes).Error
	return nodes, err
}

// UpdateState 更新状态，并且设置上次上报时间为现在
func (*NodeDB) UpdateState(ctx SQLContext, nodeID cdssdk.NodeID, state string) error {
	err := ctx.
		Model(&cdssdk.Node{}).
		Where("NodeID = ?", nodeID).
		Updates(map[string]interface{}{
			"State":          state,
			"LastReportTime": time.Now(),
		}).Error
	return err
}
