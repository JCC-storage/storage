package db

/*
import (
	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type PackageAccessStatDB struct {
	*DB
}

func (db *DB) PackageAccessStat() *PackageAccessStatDB {
	return &PackageAccessStatDB{db}
}

func (*PackageAccessStatDB) Get(ctx SQLContext, pkgID cdssdk.PackageID, hubID cdssdk.HubID) (stgmod.PackageAccessStat, error) {
	var ret stgmod.PackageAccessStat
	err := sqlx.Get(ctx, &ret, "select * from PackageAccessStat where PackageID=? and HubID=?", pkgID, hubID)
	return ret, err
}

func (*PackageAccessStatDB) GetByPackageID(ctx SQLContext, pkgID cdssdk.PackageID) ([]stgmod.PackageAccessStat, error) {
	var ret []stgmod.PackageAccessStat
	err := sqlx.Select(ctx, &ret, "select * from PackageAccessStat where PackageID=?", pkgID)
	return ret, err
}

func (*PackageAccessStatDB) BatchGetByPackageID(ctx SQLContext, pkgIDs []cdssdk.PackageID) ([]stgmod.PackageAccessStat, error) {
	if len(pkgIDs) == 0 {
		return nil, nil
	}

	var ret []stgmod.PackageAccessStat
	stmt, args, err := sqlx.In("select * from PackageAccessStat where PackageID in (?)", pkgIDs)
	if err != nil {
		return nil, err
	}

	err = sqlx.Select(ctx, &ret, stmt, args...)
	return ret, err
}

func (*PackageAccessStatDB) BatchAddCounter(ctx SQLContext, entries []coormq.AddAccessStatEntry) error {
	if len(entries) == 0 {
		return nil
	}

	sql := "insert into PackageAccessStat(PackageID, HubID, Counter, Amount)" +
		" values(:PackageID, :HubID, :Counter, 0) as new" +
		" on duplicate key update Counter=Counter+new.Counter"
	err := BatchNamedExec(ctx, sql, 4, entries, nil)
	return err
}

func (*PackageAccessStatDB) BatchUpdateAmount(ctx SQLContext, pkgIDs []cdssdk.PackageID, historyWeight float64) error {
	if len(pkgIDs) == 0 {
		return nil
	}

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
*/
