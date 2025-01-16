package db2

import (
	"strconv"
	"strings"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gorm.io/gorm/clause"
)

type ObjectBlockDB struct {
	*DB
}

func (db *DB) ObjectBlock() *ObjectBlockDB {
	return &ObjectBlockDB{DB: db}
}

func (db *ObjectBlockDB) GetByStorageID(ctx SQLContext, stgID cdssdk.StorageID) ([]stgmod.ObjectBlock, error) {
	var rets []stgmod.ObjectBlock
	err := ctx.Table("ObjectBlock").Where("StorageID = ?", stgID).Find(&rets).Error
	return rets, err
}

func (db *ObjectBlockDB) BatchGetByObjectID(ctx SQLContext, objectIDs []cdssdk.ObjectID) ([]stgmod.ObjectBlock, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}

	var blocks []stgmod.ObjectBlock
	err := ctx.Table("ObjectBlock").Where("ObjectID IN (?)", objectIDs).Order("ObjectID, `Index` ASC").Find(&blocks).Error
	return blocks, err
}

func (*ObjectBlockDB) GetInPackageID(ctx SQLContext, packageID cdssdk.PackageID) ([]stgmod.ObjectBlock, error) {
	var rets []stgmod.ObjectBlock
	err := ctx.Table("ObjectBlock").
		Joins("INNER JOIN Object ON ObjectBlock.ObjectID = Object.ObjectID").
		Where("Object.PackageID = ?", packageID).
		Order("ObjectBlock.ObjectID, ObjectBlock.`Index` ASC").
		Find(&rets).Error
	return rets, err
}

func (db *ObjectBlockDB) Create(ctx SQLContext, objectID cdssdk.ObjectID, index int, stgID cdssdk.StorageID, fileHash cdssdk.FileHash) error {
	block := stgmod.ObjectBlock{ObjectID: objectID, Index: index, StorageID: stgID, FileHash: fileHash}
	return ctx.Table("ObjectBlock").Create(&block).Error
}

func (db *ObjectBlockDB) BatchCreate(ctx SQLContext, blocks []stgmod.ObjectBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	return ctx.Clauses(clause.Insert{Modifier: "ignore"}).Create(&blocks).Error
}

func (db *ObjectBlockDB) DeleteByObjectID(ctx SQLContext, objectID cdssdk.ObjectID) error {
	return ctx.Table("ObjectBlock").Where("ObjectID = ?", objectID).Delete(&stgmod.ObjectBlock{}).Error
}

func (db *ObjectBlockDB) BatchDeleteByObjectID(ctx SQLContext, objectIDs []cdssdk.ObjectID) error {
	if len(objectIDs) == 0 {
		return nil
	}

	return ctx.Table("ObjectBlock").Where("ObjectID IN (?)", objectIDs).Delete(&stgmod.ObjectBlock{}).Error
}

func (db *ObjectBlockDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	return ctx.Table("ObjectBlock").Where("ObjectID IN (SELECT ObjectID FROM Object WHERE PackageID = ?)", packageID).Delete(&stgmod.ObjectBlock{}).Error
}

func (db *ObjectBlockDB) StorageBatchDelete(ctx SQLContext, stgID cdssdk.StorageID, fileHashes []cdssdk.FileHash) error {
	if len(fileHashes) == 0 {
		return nil
	}

	return ctx.Table("ObjectBlock").Where("StorageID = ? AND FileHash IN (?)", stgID, fileHashes).Delete(&stgmod.ObjectBlock{}).Error
}

func (db *ObjectBlockDB) CountBlockWithHash(ctx SQLContext, fileHash string) (int, error) {
	var cnt int64
	err := ctx.Table("ObjectBlock").
		Select("COUNT(FileHash)").
		Joins("INNER JOIN Object ON ObjectBlock.ObjectID = Object.ObjectID").
		Joins("INNER JOIN Package ON Object.PackageID = Package.PackageID").
		Where("FileHash = ? AND Package.State = ?", fileHash, cdssdk.PackageStateNormal).
		Scan(&cnt).Error

	if err != nil {
		return 0, err
	}

	return int(cnt), nil
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
