package db

/*
import (
	"time"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type CacheDB struct {
	*DB
}

func (db *DB) Cache() *CacheDB {
	return &CacheDB{DB: db}
}

func (*CacheDB) Get(ctx SQLContext, fileHash string, hubID cdssdk.HubID) (model.Cache, error) {
	var ret model.Cache
	err := sqlx.Get(ctx, &ret, "select * from Cache where FileHash = ? and HubID = ?", fileHash, hubID)
	return ret, err
}

func (*CacheDB) BatchGetAllFileHashes(ctx SQLContext, start int, count int) ([]string, error) {
	var ret []string
	err := sqlx.Select(ctx, &ret, "select distinct FileHash from Cache limit ?, ?", start, count)
	return ret, err
}

func (*CacheDB) GetByHubID(ctx SQLContext, hubID cdssdk.HubID) ([]model.Cache, error) {
	var ret []model.Cache
	err := sqlx.Select(ctx, &ret, "select * from Cache where HubID = ?", hubID)
	return ret, err
}

// Create 创建一条的缓存记录，如果已有则不进行操作
func (*CacheDB) Create(ctx SQLContext, fileHash string, hubID cdssdk.HubID, priority int) error {
	_, err := ctx.Exec("insert ignore into Cache values(?,?,?,?)", fileHash, hubID, time.Now(), priority)
	if err != nil {
		return err
	}

	return nil
}

// 批量创建缓存记录
func (*CacheDB) BatchCreate(ctx SQLContext, caches []model.Cache) error {
	if len(caches) == 0 {
		return nil
	}
	return BatchNamedExec(
		ctx,
		"insert into Cache(FileHash,HubID,CreateTime,Priority) values(:FileHash,:HubID,:CreateTime,:Priority)"+
			" on duplicate key update CreateTime=values(CreateTime), Priority=values(Priority)",
		4,
		caches,
		nil,
	)
}

func (*CacheDB) BatchCreateOnSameNode(ctx SQLContext, fileHashes []string, hubID cdssdk.HubID, priority int) error {
	if len(fileHashes) == 0 {
		return nil
	}

	var caches []model.Cache
	var nowTime = time.Now()
	for _, hash := range fileHashes {
		caches = append(caches, model.Cache{
			FileHash:   hash,
			HubID:     hubID,
			CreateTime: nowTime,
			Priority:   priority,
		})
	}

	return BatchNamedExec(ctx,
		"insert into Cache(FileHash,HubID,CreateTime,Priority) values(:FileHash,:HubID,:CreateTime,:Priority)"+
			" on duplicate key update CreateTime=values(CreateTime), Priority=values(Priority)",
		4,
		caches,
		nil,
	)
}

func (*CacheDB) NodeBatchDelete(ctx SQLContext, hubID cdssdk.HubID, fileHashes []string) error {
	if len(fileHashes) == 0 {
		return nil
	}

	// TODO in语句有长度限制
	query, args, err := sqlx.In("delete from Cache where HubID = ? and FileHash in (?)", hubID, fileHashes)
	if err != nil {
		return err
	}
	_, err = ctx.Exec(query, args...)
	return err
}

// GetCachingFileNodes 查找缓存了指定文件的节点
func (*CacheDB) GetCachingFileNodes(ctx SQLContext, fileHash string) ([]cdssdk.Node, error) {
	var x []cdssdk.Node
	err := sqlx.Select(ctx, &x,
		"select Node.* from Cache, Node where Cache.FileHash=? and Cache.HubID = Node.HubID", fileHash)
	return x, err
}

// DeleteNodeAll 删除一个节点所有的记录
func (*CacheDB) DeleteNodeAll(ctx SQLContext, hubID cdssdk.HubID) error {
	_, err := ctx.Exec("delete from Cache where HubID = ?", hubID)
	return err
}

// FindCachingFileUserNodes 在缓存表中查询指定数据所在的节点
func (*CacheDB) FindCachingFileUserNodes(ctx SQLContext, userID cdssdk.HubID, fileHash string) ([]cdssdk.Node, error) {
	var x []cdssdk.Node
	err := sqlx.Select(ctx, &x,
		"select Node.* from Cache, UserNode, Node where"+
			" Cache.FileHash=? and Cache.HubID = UserNode.HubID and"+
			" UserNode.UserID = ? and UserNode.HubID = Node.HubID", fileHash, userID)
	return x, err
}
*/
