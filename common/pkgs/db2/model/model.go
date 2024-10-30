package model

import (
	"fmt"
	"reflect"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/serder"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

// TODO 可以考虑逐步迁移到cdssdk中。迁移思路：数据对象应该包含的字段都迁移到cdssdk中，内部使用的一些特殊字段则留在这里
type Storage = cdssdk.Storage

type User struct {
	UserID   cdssdk.UserID `gorm:"column:UserID" json:"userID"`
	Password string        `gorm:"column:Password" json:"password"`
}

func (User) TableName() string {
	return "User"
}

type UserBucket struct {
	UserID   cdssdk.UserID   `gorm:"column:UserID; primaryKey" json:"userID"`
	BucketID cdssdk.BucketID `gorm:"column:BucketID; primaryKey" json:"bucketID"`
}

func (UserBucket) TableName() string {
	return "UserBucket"
}

type UserNode struct {
	UserID cdssdk.UserID `gorm:"column:UserID; primaryKey" json:"userID"`
	NodeID cdssdk.NodeID `gorm:"column:NodeID; primaryKey" json:"nodeID"`
}

func (UserNode) TableName() string {
	return "UserNode"
}

type UserStorage struct {
	UserID    cdssdk.UserID    `gorm:"column:UserID; primaryKey" json:"userID"`
	StorageID cdssdk.StorageID `gorm:"column:StorageID; primaryKey" json:"storageID"`
}

func (UserStorage) TableName() string {
	return "UserStorage"
}

type Bucket = cdssdk.Bucket

type Package = cdssdk.Package

type Object = cdssdk.Object

type NodeConnectivity = cdssdk.NodeConnectivity

// 由于Object的Redundancy字段是interface，所以不能直接将查询结果scan成Object，必须先scan成TempObject，
// 再.ToObject()转成Object
type TempObject struct {
	cdssdk.Object
	Redundancy RedundancyWarpper `db:"Redundancy"`
}

func (o *TempObject) ToObject() cdssdk.Object {
	obj := o.Object
	obj.Redundancy = o.Redundancy.Value
	return obj
}

type RedundancyWarpper struct {
	Value cdssdk.Redundancy
}

func (o *RedundancyWarpper) Scan(src interface{}) error {
	data, ok := src.([]uint8)
	if !ok {
		return fmt.Errorf("unknow src type: %v", reflect.TypeOf(data))
	}

	red, err := serder.JSONToObjectEx[cdssdk.Redundancy](data)
	if err != nil {
		return err
	}

	o.Value = red
	return nil
}

type ObjectBlock = stgmod.ObjectBlock

type Cache struct {
	FileHash   cdssdk.FileHash  `gorm:"column:FileHash; primaryKey" json:"fileHash"`
	StorageID  cdssdk.StorageID `gorm:"column:StorageID; primaryKey" json:"storageID"`
	CreateTime time.Time        `gorm:"column:CreateTime" json:"createTime"`
	Priority   int              `gorm:"column:Priority" json:"priority"`
}

func (Cache) TableName() string {
	return "Cache"
}

const (
	StoragePackageStateNormal   = "Normal"
	StoragePackageStateDeleted  = "Deleted"
	StoragePackageStateOutdated = "Outdated"
)

// Storage当前加载的Package
type StoragePackage struct {
	StorageID cdssdk.StorageID `gorm:"column:StorageID; primaryKey" json:"storageID"`
	PackageID cdssdk.PackageID `gorm:"column:PackageID; primaryKey" json:"packageID"`
	UserID    cdssdk.UserID    `gorm:"column:UserID; primaryKey" json:"userID"`
	State     string           `gorm:"column:State" json:"state"`
}

func (StoragePackage) TableName() string {
	return "StoragePackage"
}

type Location struct {
	LocationID cdssdk.LocationID `gorm:"column:LocationID; primaryKey; autoIncrement" json:"locationID"`
	Name       string            `gorm:"column:Name" json:"name"`
}

func (Location) TableName() string {
	return "Location"
}
