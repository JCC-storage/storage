package db

/*
import (
	"time"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type NodeDB struct {
	*DB
}

func (db *DB) Node() *NodeDB {
	return &NodeDB{DB: db}
}

func (db *NodeDB) GetByID(ctx SQLContext, hubID cdssdk.HubID) (cdssdk.Node, error) {
	var ret cdssdk.Node
	err := sqlx.Get(ctx, &ret, "select * from Node where HubID = ?", hubID)
	return ret, err
}

func (db *NodeDB) GetAllNodes(ctx SQLContext) ([]cdssdk.Node, error) {
	var ret []cdssdk.Node
	err := sqlx.Select(ctx, &ret, "select * from Node")
	return ret, err
}

// GetUserNodes 根据用户id查询可用node
func (db *NodeDB) GetUserNodes(ctx SQLContext, userID cdssdk.UserID) ([]cdssdk.Node, error) {
	var nodes []cdssdk.Node
	err := sqlx.Select(ctx, &nodes, "select Node.* from UserNode, Node where UserNode.HubID = Node.HubID and UserNode.UserID=?", userID)
	return nodes, err
}

// UpdateState 更新状态，并且设置上次上报时间为现在
func (db *NodeDB) UpdateState(ctx SQLContext, hubID cdssdk.HubID, state string) error {
	_, err := ctx.Exec("update Node set State = ?, LastReportTime = ? where HubID = ?", state, time.Now(), hubID)
	return err
}
*/
