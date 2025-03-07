package db

/*
import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type ObjectBlockDB struct {
	*DB
}

func (db *DB) ObjectBlock() *ObjectBlockDB {
	return &ObjectBlockDB{DB: db}
}

func (db *ObjectBlockDB) GetByHubID(ctx SQLContext, hubID cdssdk.HubID) ([]stgmod.ObjectBlock, error) {
	var rets []stgmod.ObjectBlock
	err := sqlx.Select(ctx, &rets, "select * from ObjectBlock where HubID = ?", hubID)
	return rets, err
}

func (db *ObjectBlockDB) BatchGetByObjectID(ctx SQLContext, objectIDs []cdssdk.ObjectID) ([]stgmod.ObjectBlock, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}

	stmt, args, err := sqlx.In("select * from ObjectBlock where ObjectID in (?) order by ObjectID, `Index` asc", objectIDs)
	if err != nil {
		return nil, err
	}
	stmt = ctx.Rebind(stmt)

	var blocks []stgmod.ObjectBlock
	err = sqlx.Select(ctx, &blocks, stmt, args...)
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

func (db *ObjectBlockDB) Create(ctx SQLContext, objectID cdssdk.ObjectID, index int, hubID cdssdk.HubID, fileHash string) error {
	_, err := ctx.Exec("insert into ObjectBlock values(?,?,?,?)", objectID, index, hubID, fileHash)
	return err
}

func (db *ObjectBlockDB) BatchCreate(ctx SQLContext, blocks []stgmod.ObjectBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	return BatchNamedExec(ctx,
		"insert ignore into ObjectBlock(ObjectID, `Index`, HubID, FileHash) values(:ObjectID, :Index, :HubID, :FileHash)",
		4,
		blocks,
		nil,
	)
}

func (db *ObjectBlockDB) DeleteByObjectID(ctx SQLContext, objectID cdssdk.ObjectID) error {
	_, err := ctx.Exec("delete from ObjectBlock where ObjectID = ?", objectID)
	return err
}

func (db *ObjectBlockDB) BatchDeleteByObjectID(ctx SQLContext, objectIDs []cdssdk.ObjectID) error {
	if len(objectIDs) == 0 {
		return nil
	}

	// TODO in语句有长度限制
	query, args, err := sqlx.In("delete from ObjectBlock where ObjectID in (?)", objectIDs)
	if err != nil {
		return err
	}
	_, err = ctx.Exec(query, args...)
	return err
}

func (db *ObjectBlockDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete ObjectBlock from ObjectBlock inner join Object on ObjectBlock.ObjectID = Object.ObjectID where PackageID = ?", packageID)
	return err
}

func (db *ObjectBlockDB) NodeBatchDelete(ctx SQLContext, hubID cdssdk.HubID, fileHashes []string) error {
	if len(fileHashes) == 0 {
		return nil
	}

	query, args, err := sqlx.In("delete from ObjectBlock where HubID = ? and FileHash in (?)", hubID, fileHashes)
	if err != nil {
		return err
	}

	_, err = ctx.Exec(query, args...)
	return err
}

func (db *ObjectBlockDB) CountBlockWithHash(ctx SQLContext, fileHash string) (int, error) {
	var cnt int
	err := sqlx.Get(ctx, &cnt,
		"select count(FileHash) from ObjectBlock, Object, Package where FileHash = ? and"+
			" ObjectBlock.ObjectID = Object.ObjectID and"+
			" Object.PackageID = Package.PackageID and"+
			" Package.State = ?", fileHash, cdssdk.PackageStateNormal)
	if err == sql.ErrNoRows {
		return 0, nil
	}

	return cnt, err
}

// 按逗号切割字符串，并将每一个部分解析为一个int64的ID。
// 注：需要外部保证分隔的每一个部分都是正确的10进制数字格式
func splitConcatedHubID(idStr string) []cdssdk.HubID {
	idStrs := strings.Split(idStr, ",")
	ids := make([]cdssdk.HubID, 0, len(idStrs))

	for _, str := range idStrs {
		// 假设传入的ID是正确的数字格式
		id, _ := strconv.ParseInt(str, 10, 64)
		ids = append(ids, cdssdk.HubID(id))
	}

	return ids
}

// 按逗号切割字符串
func splitConcatedFileHash(idStr string) []string {
	idStrs := strings.Split(idStr, ",")
	return idStrs
}
*/
