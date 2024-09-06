package db

import (
	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type PackageAccessStatDB struct {
	*DB
}

func (db *DB) PackageAccessStat() *PackageAccessStatDB {
	return &PackageAccessStatDB{db}
}

func (*PackageAccessStatDB) Get(ctx SQLContext, pkgID cdssdk.PackageID, nodeID cdssdk.NodeID) (model.PackageAccessStat, error) {
	var ret model.PackageAccessStat
	err := sqlx.Get(ctx, &ret, "select * from PackageAccessStat where PackageID=? and NodeID=?", pkgID, nodeID)
	return ret, err
}

func (*PackageAccessStatDB) GetByPackageID(ctx SQLContext, pkgID cdssdk.PackageID) ([]model.PackageAccessStat, error) {
	var ret []model.PackageAccessStat
	err := sqlx.Select(ctx, &ret, "select * from PackageAccessStat where PackageID=?", pkgID)
	return ret, err
}

func (*PackageAccessStatDB) BatchAddCounter(ctx SQLContext, entries []coormq.AddPackageAccessStatCounterEntry) error {
	sql := "insert into PackageAccessStat(PackageID, NodeID, Counter, Amount) " +
		"values(:PackageID, :NodeID, :Value, 0)" +
		"on duplicate key update Counter=Counter+:Value"
	err := BatchNamedExec(ctx, sql, 4, entries, nil)
	return err
}

func (*PackageAccessStatDB) BatchUpdateAmount(ctx SQLContext, pkgIDs []cdssdk.PackageID, historyWeight float64) error {
	stmt, args, err := sqlx.In("update PackageAccessStat set Amount=Amount*?+Counter*(1-?), Counter = 0 where PackageID in (?)", historyWeight, historyWeight, pkgIDs)
	if err != nil {
		return err
	}

	_, err = ctx.Exec(stmt, args...)
	return err
}

func (*PackageAccessStatDB) UpdateAllAmount(ctx SQLContext, historyWeight float64) error {
	stmt, args, err := sqlx.In("update PackageAccessStat set Amount=Amount*?+Counter*(1-?), Counter = 0", historyWeight, historyWeight)
	if err != nil {
		return err
	}

	_, err = ctx.Exec(stmt, args...)
	return err
}

func (*PackageAccessStatDB) DeleteByPackageID(ctx SQLContext, pkgID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete from PackageAccessStat where PackageID=?", pkgID)
	return err
}
