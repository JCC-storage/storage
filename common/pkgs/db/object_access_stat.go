package db

/*
import (
	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type ObjectAccessStatDB struct {
	*DB
}

func (db *DB) ObjectAccessStat() *ObjectAccessStatDB {
	return &ObjectAccessStatDB{db}
}

func (*ObjectAccessStatDB) Get(ctx SQLContext, objID cdssdk.ObjectID, hubID cdssdk.HubID) (stgmod.ObjectAccessStat, error) {
	var ret stgmod.ObjectAccessStat
	err := sqlx.Get(ctx, &ret, "select * from ObjectAccessStat where ObjectID=? and HubID=?", objID, hubID)
	return ret, err
}

func (*ObjectAccessStatDB) GetByObjectID(ctx SQLContext, objID cdssdk.ObjectID) ([]stgmod.ObjectAccessStat, error) {
	var ret []stgmod.ObjectAccessStat
	err := sqlx.Select(ctx, &ret, "select * from ObjectAccessStat where ObjectID=?", objID)
	return ret, err
}

func (*ObjectAccessStatDB) BatchGetByObjectID(ctx SQLContext, objIDs []cdssdk.ObjectID) ([]stgmod.ObjectAccessStat, error) {
	if len(objIDs) == 0 {
		return nil, nil
	}

	var ret []stgmod.ObjectAccessStat
	stmt, args, err := sqlx.In("select * from ObjectAccessStat where ObjectID in (?)", objIDs)
	if err != nil {
		return ret, err
	}

	err = sqlx.Select(ctx, &ret, stmt, args...)
	return ret, err
}

func (*ObjectAccessStatDB) BatchGetByObjectIDOnNode(ctx SQLContext, objIDs []cdssdk.ObjectID, hubID cdssdk.HubID) ([]stgmod.ObjectAccessStat, error) {
	if len(objIDs) == 0 {
		return nil, nil
	}

	var ret []stgmod.ObjectAccessStat
	stmt, args, err := sqlx.In("select * from ObjectAccessStat where ObjectID in (?) and HubID=?", objIDs, hubID)
	if err != nil {
		return ret, err
	}

	err = sqlx.Select(ctx, &ret, stmt, args...)
	return ret, err
}

func (*ObjectAccessStatDB) BatchAddCounter(ctx SQLContext, entries []coormq.AddAccessStatEntry) error {
	if len(entries) == 0 {
		return nil
	}

	sql := "insert into ObjectAccessStat(ObjectID, HubID, Counter, Amount) " +
		" values(:ObjectID, :HubID, :Counter, 0) as new" +
		" on duplicate key update ObjectAccessStat.Counter=ObjectAccessStat.Counter+new.Counter"
	err := BatchNamedExec(ctx, sql, 4, entries, nil)
	return err
}

func (*ObjectAccessStatDB) BatchUpdateAmountInPackage(ctx SQLContext, pkgIDs []cdssdk.PackageID, historyWeight float64) error {
	if len(pkgIDs) == 0 {
		return nil
	}

	stmt, args, err := sqlx.In("update ObjectAccessStat inner join Object"+
		" on ObjectAccessStat.ObjectID = Object.ObjectID"+
		" set Amount=Amount*?+Counter*(1-?), Counter = 0"+
		" where PackageID in (?)", historyWeight, historyWeight, pkgIDs)
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
	if len(objIDs) == 0 {
		return nil
	}

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
*/
