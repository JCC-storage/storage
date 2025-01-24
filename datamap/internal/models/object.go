package models

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
	"gorm.io/gorm"
)

type Object struct {
	ObjectID       cdssdk.ObjectID  `gorm:"column:ObjectID; primaryKey; type:bigint; autoIncrement" json:"objectID"`
	PackageID      cdssdk.PackageID `gorm:"column:PackageID; type:bigint; not null" json:"packageID"`
	Path           string           `gorm:"column:Path; type:varchar(1024); not null" json:"path"`
	Size           int64            `gorm:"column:Size; type:bigint; not null" json:"size"`
	FileHash       string           `gorm:"column:FileHash; type:varchar(255); not null" json:"fileHash"`
	Status         Status           `gorm:"column:Status; type:tinyint; not null" json:"status"`
	FaultTolerance float64          `gorm:"column:faultTolerance; type:float; not null" json:"faultTolerance"`
	Redundancy     float64          `gorm:"column:redundancy; type:float; not null" json:"redundancy"`
	AvgAccessCost  float64          `gorm:"column:avgAccessCost; type:float; not null" json:"avgAccessCost"`
	Timestamp      time.Time        `gorm:"column:Timestamp; type:datatime; not null" json:"timestamp"`
}

func (Object) TableName() string {
	return "object"
}

// ObjectRepository 块传输记录的 Repository
type ObjectRepository struct {
	repo *GormRepository
}

// NewObjectRepository 创建 ObjectRepository 实例
func NewObjectRepository(db *gorm.DB) *ObjectRepository {
	return &ObjectRepository{repo: NewGormRepository(db)}
}

// CreateObject 创建块传输记录
func (r *ObjectRepository) CreateObject(object *Object) error {
	return r.repo.Create(object)
}

// GetObjectByID 查询单个Object
func (r *ObjectRepository) GetObjectByID(objectID int64) (*Object, error) {
	var object Object
	query := "SELECT * FROM object WHERE ObjectID = ?"
	err := r.repo.db.Raw(query, objectID).First(&object).Error
	if err != nil {
		return nil, err
	}
	return &object, nil
}

// UpdateObject 更新块传输记录
func (r *ObjectRepository) UpdateObject(object *Object) error {
	return r.repo.Update(object)
}

// GetAllObjects 获取所有块传输记录
func (r *ObjectRepository) GetAllObjects() ([]Object, error) {
	var objects []Object
	err := r.repo.GetAll(&objects)
	if err != nil {
		return nil, err
	}
	return objects, nil
}

type ObjectWatcher struct {
	Name string
}

func (w *ObjectWatcher) OnEvent(event sysevent.SysEvent) {

	if event.Category == "objectChange" {
		if _, ok := event.Body.(*stgmod.BodyNewObject); ok {

		} else {
			fmt.Printf("Watcher %s: Unexpected Body type, expected *ObjectInfo, got %T\n", w.Name, event.Body)
		}
	} else {
		fmt.Printf("Watcher %s received an event with category %s\n", w.Name, event.Category)
	}
}
