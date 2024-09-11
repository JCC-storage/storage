package db

import (
	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type ObjectAccessStatDB struct {
	*DB
}

func (db *DB) ObjectAccessStat() *ObjectAccessStatDB {
	return &ObjectAccessStatDB{db}
}

func (*ObjectAccessStatDB) Get(ctx SQLContext, objID cdssdk.ObjectID, nodeID cdssdk.NodeID) (model.ObjectAccessStat, error) {
	var ret model.ObjectAccessStat
	err := sqlx.Get(ctx, &ret, "select * from ObjectAccessStat where ObjectID=? and NodeID=?", objID, nodeID)
	return ret, err
}

func (*ObjectAccessStatDB) GetByObjectID(ctx SQLContext, objID cdssdk.ObjectID) ([]model.ObjectAccessStat, error) {
	var ret []model.ObjectAccessStat
	err := sqlx.Select(ctx, &ret, "select * from ObjectAccessStat where ObjectID=?", objID)
	return ret, err
}

func (*ObjectAccessStatDB) BatchAddCounter(ctx SQLContext, entries []coormq.AddAccessStatEntry) error {
	sql := "insert into ObjectAccessStat(ObjectID, NodeID, Counter, Amount) " +
		"values(:ObjectID, :NodeID, :Counter, 0) as new" +
		"on duplicate key update Counter=Counter+new.Counter"
	err := BatchNamedExec(ctx, sql, 4, entries, nil)
	return err
}

func (*ObjectAccessStatDB) BatchUpdateAmount(ctx SQLContext, objIDs []cdssdk.ObjectID, historyWeight float64) error {
	stmt, args, err := sqlx.In("update ObjectAccessStat set Amount=Amount*?+Counter*(1-?), Counter = 0 where ObjectID in (?)", historyWeight, historyWeight, objIDs)
	if err != nil {
		return err
	}

	_, err = ctx.Exec(stmt, args...)
	return err
}

func (*ObjectAccessStatDB) UpdateAllAmount(ctx SQLContext, historyWeight float64) error {
	stmt, args, err := sqlx.In("update ObjectAccessStat set Amount=Amount*?+Counter*(1-?), Counter = 0", historyWeight, historyWeight)
	if err != nil {
		return err
	}

	_, err = ctx.Exec(stmt, args...)
	return err
}

func (*ObjectAccessStatDB) DeleteByObjectID(ctx SQLContext, objID cdssdk.ObjectID) error {
	_, err := ctx.Exec("delete from ObjectAccessStat where ObjectID=?", objID)
	return err
}

func (*ObjectAccessStatDB) BatchDeleteByObjectID(ctx SQLContext, objIDs []cdssdk.ObjectID) error {
	stmt, args, err := sqlx.In("delete from ObjectAccessStat where ObjectID in (?)", objIDs)
	if err != nil {
		return err
	}

	_, err = ctx.Exec(stmt, args...)
	return err
}

func (*ObjectAccessStatDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete ObjectAccessStat from ObjectAccessStat inner join Object on ObjectAccessStat.ObjectID = Object.ObjectID where PackageID = ?", packageID)
	return err
}
