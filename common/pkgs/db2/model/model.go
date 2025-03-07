package model

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

// TODO 可以考虑逐步迁移到cdssdk中。迁移思路：数据对象应该包含的字段都迁移到cdssdk中，内部使用的一些特殊字段则留在这里
type Storage = cdssdk.Storage

type UserBucket struct {
	UserID   cdssdk.UserID   `gorm:"column:UserID; primaryKey; type:bigint" json:"userID"`
	BucketID cdssdk.BucketID `gorm:"column:BucketID; primaryKey; type:bigint" json:"bucketID"`
}

func (UserBucket) TableName() string {
	return "UserBucket"
}

type UserHub struct {
	UserID cdssdk.UserID `gorm:"column:UserID; primaryKey; type:bigint" json:"userID"`
	HubID  cdssdk.HubID  `gorm:"column:HubID; primaryKey; type:bigint" json:"hubID"`
}

func (UserHub) TableName() string {
	return "UserHub"
}

type UserStorage struct {
	UserID    cdssdk.UserID    `gorm:"column:UserID; primaryKey; type:bigint" json:"userID"`
	StorageID cdssdk.StorageID `gorm:"column:StorageID; primaryKey; type:bigint" json:"storageID"`
}

func (UserStorage) TableName() string {
	return "UserStorage"
}

type Bucket = cdssdk.Bucket

type Package = cdssdk.Package

type Object = cdssdk.Object

type HubConnectivity = cdssdk.HubConnectivity

type ObjectBlock = stgmod.ObjectBlock

type Cache struct {
	FileHash   cdssdk.FileHash  `gorm:"column:FileHash; primaryKey; type: char(68)" json:"fileHash"`
	StorageID  cdssdk.StorageID `gorm:"column:StorageID; primaryKey; type: bigint" json:"storageID"`
	CreateTime time.Time        `gorm:"column:CreateTime; type:datetime" json:"createTime"`
	Priority   int              `gorm:"column:Priority; type:int" json:"priority"`
}

func (Cache) TableName() string {
	return "Cache"
}

type Location struct {
	LocationID cdssdk.LocationID `gorm:"column:LocationID; primaryKey; type:bigint; autoIncrement" json:"locationID"`
	Name       string            `gorm:"column:Name; type:varchar(255); not null" json:"name"`
}

func (Location) TableName() string {
	return "Location"
}
