package db2

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gorm.io/gorm/clause"
)

type CacheDB struct {
	*DB
}

func (db *DB) Cache() *CacheDB {
	return &CacheDB{DB: db}
}

func (*CacheDB) Get(ctx SQLContext, fileHash cdssdk.FileHash, stgID cdssdk.StorageID) (model.Cache, error) {
	var ret model.Cache
	err := ctx.Table("Cache").Where("FileHash = ? AND StorageID = ?", fileHash, stgID).First(&ret).Error
	return ret, err
}

func (*CacheDB) BatchGetAllFileHashes(ctx SQLContext, start int, count int) ([]string, error) {
	var ret []string
	err := ctx.Table("Cache").Distinct("FileHash").Offset(start).Limit(count).Pluck("FileHash", &ret).Error
	return ret, err
}

func (*CacheDB) GetByStorageID(ctx SQLContext, stgID cdssdk.StorageID) ([]model.Cache, error) {
	var ret []model.Cache
	err := ctx.Table("Cache").Where("StorageID = ?", stgID).Find(&ret).Error
	return ret, err
}

// Create 创建一条缓存记录，如果已有则不进行操作
func (*CacheDB) Create(ctx SQLContext, fileHash cdssdk.FileHash, stgID cdssdk.StorageID, priority int) error {
	cache := model.Cache{FileHash: fileHash, StorageID: stgID, CreateTime: time.Now(), Priority: priority}
	return ctx.Where(cache).Attrs(cache).FirstOrCreate(&cache).Error
}

// 批量创建缓存记录
func (*CacheDB) BatchCreate(ctx SQLContext, caches []model.Cache) error {
	if len(caches) == 0 {
		return nil
	}

	return ctx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "FileHash"}, {Name: "StorageID"}},
		DoUpdates: clause.AssignmentColumns([]string{"CreateTime", "Priority"}),
	}).Create(&caches).Error
}

func (db *CacheDB) BatchCreateOnSameStorage(ctx SQLContext, fileHashes []cdssdk.FileHash, stgID cdssdk.StorageID, priority int) error {
	if len(fileHashes) == 0 {
		return nil
	}

	var caches []model.Cache
	var nowTime = time.Now()
	for _, hash := range fileHashes {
		caches = append(caches, model.Cache{
			FileHash:   hash,
			StorageID:  stgID,
			CreateTime: nowTime,
			Priority:   priority,
		})
	}

	return db.BatchCreate(ctx, caches)
}

func (*CacheDB) StorageBatchDelete(ctx SQLContext, stgID cdssdk.StorageID, fileHashes []cdssdk.FileHash) error {
	if len(fileHashes) == 0 {
		return nil
	}

	return ctx.Table("Cache").Where("StorageID = ? AND FileHash IN (?)", stgID, fileHashes).Delete(&model.Cache{}).Error
}

// GetCachingFileStorages 查找缓存了指定文件的存储服务
func (*CacheDB) GetCachingFileStorages(ctx SQLContext, fileHash cdssdk.FileHash) ([]cdssdk.Storage, error) {
	var stgs []cdssdk.Storage
	err := ctx.Table("Cache").Select("Storage.*").
		Joins("JOIN Storage ON Cache.StorageID = Storage.StorageID").
		Where("Cache.FileHash = ?", fileHash).
		Find(&stgs).Error
	return stgs, err
}

// DeleteStorageAll 删除一个存储服务所有的记录
func (*CacheDB) DeleteStorageAll(ctx SQLContext, StorageID cdssdk.StorageID) error {
	return ctx.Where("StorageID = ?", StorageID).Delete(&model.Cache{}).Error
}

// FindCachingFileUserStorages 在缓存表中查询指定数据所在的节点
func (*CacheDB) FindCachingFileUserStorages(ctx SQLContext, userID cdssdk.UserID, fileHash string) ([]cdssdk.Storage, error) {
	var stgs []cdssdk.Storage
	err := ctx.Table("Cache").Select("Storage.*").
		Joins("JOIN UserStorage ON Cache.StorageID = UserStorage.StorageID").
		Where("Cache.FileHash = ? AND UserStorage.UserID = ?", fileHash, userID).
		Find(&stgs).Error
	return stgs, err
}
