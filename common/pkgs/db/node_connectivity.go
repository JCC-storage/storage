package db

/*
import (
	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type NodeConnectivityDB struct {
	*DB
}

func (db *DB) NodeConnectivity() *NodeConnectivityDB {
	return &NodeConnectivityDB{DB: db}
}

func (db *NodeConnectivityDB) BatchGetByFromNode(ctx SQLContext, fromHubIDs []cdssdk.HubID) ([]model.NodeConnectivity, error) {
	if len(fromHubIDs) == 0 {
		return nil, nil
	}

	var ret []model.NodeConnectivity

	sql, args, err := sqlx.In("select * from NodeConnectivity where FromHubID in (?)", fromHubIDs)
	if err != nil {
		return nil, err
	}

	return ret, sqlx.Select(ctx, &ret, sql, args...)
}

func (db *NodeConnectivityDB) BatchUpdateOrCreate(ctx SQLContext, cons []model.NodeConnectivity) error {
	if len(cons) == 0 {
		return nil
	}

	return BatchNamedExec(ctx,
		"insert into NodeConnectivity(FromHubID, ToHubID, Delay, TestTime) values(:FromHubID, :ToHubID, :Delay, :TestTime) as new"+
			" on duplicate key update Delay = new.Delay, TestTime = new.TestTime", 4, cons, nil)
}
*/
