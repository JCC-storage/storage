package db2

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type CacheDB struct {
	*DB
}

func (db *DB) Cache() *CacheDB {
	return &CacheDB{DB: db}
}

func (*CacheDB) Get(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID) (model.Cache, error) {
	var ret model.Cache
	err := ctx.Table("Cache").Where("FileHash = ? AND NodeID = ?", fileHash, nodeID).First(&ret).Error
	return ret, err
}

func (*CacheDB) BatchGetAllFileHashes(ctx SQLContext, start int, count int) ([]string, error) {
	var ret []string
	err := ctx.Table("Cache").Distinct("FileHash").Offset(start).Limit(count).Pluck("FileHash", &ret).Error
	return ret, err
}

func (*CacheDB) GetByNodeID(ctx SQLContext, nodeID cdssdk.NodeID) ([]model.Cache, error) {
	var ret []model.Cache
	err := ctx.Table("Cache").Where("NodeID = ?", nodeID).Find(&ret).Error
	return ret, err
}

// Create 创建一条缓存记录，如果已有则不进行操作
func (*CacheDB) Create(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID, priority int) error {
	cache := model.Cache{FileHash: fileHash, NodeID: nodeID, CreateTime: time.Now(), Priority: priority}
	return ctx.Where(cache).Attrs(cache).FirstOrCreate(&cache).Error
}

// 批量创建缓存记录
func (*CacheDB) BatchCreate(ctx SQLContext, caches []model.Cache) error {
	if len(caches) == 0 {
		return nil
	}
	return BatchNamedExec(
		ctx,
		"insert into Cache(FileHash,NodeID,CreateTime,Priority) values(:FileHash,:NodeID,:CreateTime,:Priority)"+
			" on duplicate key update CreateTime=values(CreateTime), Priority=values(Priority)",
		4,
		caches,
		nil,
	)
}

func (*CacheDB) BatchCreateOnSameNode(ctx SQLContext, fileHashes []string, nodeID cdssdk.NodeID, priority int) error {
	if len(fileHashes) == 0 {
		return nil
	}

	var caches []model.Cache
	var nowTime = time.Now()
	for _, hash := range fileHashes {
		caches = append(caches, model.Cache{
			FileHash:   hash,
			NodeID:     nodeID,
			CreateTime: nowTime,
			Priority:   priority,
		})
	}

	return BatchNamedExec(ctx,
		"insert into Cache(FileHash,NodeID,CreateTime,Priority) values(:FileHash,:NodeID,:CreateTime,:Priority)"+
			" on duplicate key update CreateTime=values(CreateTime), Priority=values(Priority)",
		4,
		caches,
		nil,
	)
}

func (*CacheDB) NodeBatchDelete(ctx SQLContext, nodeID cdssdk.NodeID, fileHashes []string) error {
	if len(fileHashes) == 0 {
		return nil
	}

	return ctx.Table("Cache").Where("NodeID = ? AND FileHash IN (?)", nodeID, fileHashes).Delete(&model.Cache{}).Error
}

// GetCachingFileNodes 查找缓存了指定文件的节点
func (*CacheDB) GetCachingFileNodes(ctx SQLContext, fileHash string) ([]cdssdk.Node, error) {
	var nodes []cdssdk.Node
	err := ctx.Table("Cache").Select("Node.*").
		Joins("JOIN Node ON Cache.NodeID = Node.NodeID").
		Where("Cache.FileHash = ?", fileHash).
		Find(&nodes).Error
	return nodes, err
}

// DeleteNodeAll 删除一个节点所有的记录
func (*CacheDB) DeleteNodeAll(ctx SQLContext, nodeID cdssdk.NodeID) error {
	return ctx.Where("NodeID = ?", nodeID).Delete(&model.Cache{}).Error
}

// FindCachingFileUserNodes 在缓存表中查询指定数据所在的节点
func (*CacheDB) FindCachingFileUserNodes(ctx SQLContext, userID cdssdk.NodeID, fileHash string) ([]cdssdk.Node, error) {
	var nodes []cdssdk.Node
	err := ctx.Table("Cache").Select("Node.*").
		Joins("JOIN UserNode ON Cache.NodeID = UserNode.NodeID").
		Joins("JOIN Node ON UserNode.NodeID = Node.NodeID").
		Where("Cache.FileHash = ? AND UserNode.UserID = ?", fileHash, userID).
		Find(&nodes).Error
	return nodes, err
}
