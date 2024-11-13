package stgmod

import (
	"github.com/samber/lo"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"
)

type ObjectBlock struct {
	ObjectID  cdssdk.ObjectID  `gorm:"column:ObjectID; primaryKey; type:bigint" json:"objectID"`
	Index     int              `gorm:"column:Index; primaryKey; type:int" json:"index"`
	StorageID cdssdk.StorageID `gorm:"column:StorageID; primaryKey; type:bigint" json:"storageID"` // 这个块应该在哪个节点上
	FileHash  cdssdk.FileHash  `gorm:"column:FileHash; type:char(64); not null" json:"fileHash"`
}

func (ObjectBlock) TableName() string {
	return "ObjectBlock"
}

type ObjectDetail struct {
	Object   cdssdk.Object      `json:"object"`
	PinnedAt []cdssdk.StorageID `json:"pinnedAt"`
	Blocks   []ObjectBlock      `json:"blocks"`
}

func NewObjectDetail(object cdssdk.Object, pinnedAt []cdssdk.StorageID, blocks []ObjectBlock) ObjectDetail {
	return ObjectDetail{
		Object:   object,
		PinnedAt: pinnedAt,
		Blocks:   blocks,
	}
}

func DetailsFromObjects(objects []cdssdk.Object) []ObjectDetail {
	details := make([]ObjectDetail, len(objects))
	for i, object := range objects {
		details[i] = ObjectDetail{
			Object: object,
		}
	}
	return details
}

// 将blocks放到对应的object中。要求objs和blocks都按ObjectID升序
func DetailsFillObjectBlocks(objs []ObjectDetail, blocks []ObjectBlock) {
	blksCur := 0
	for i := range objs {
		obj := &objs[i]
		// 1. 查询Object和ObjectBlock时均按照ObjectID升序排序
		// 2. ObjectBlock结果集中的不同ObjectID数只会比Object结果集的少
		// 因此在两个结果集上同时从头开始遍历时，如果两边的ObjectID字段不同，那么一定是ObjectBlock这边的ObjectID > Object的ObjectID，
		// 此时让Object的遍历游标前进，直到两边的ObjectID再次相等
		for ; blksCur < len(blocks); blksCur++ {
			if blocks[blksCur].ObjectID != obj.Object.ObjectID {
				break
			}
			obj.Blocks = append(obj.Blocks, blocks[blksCur])
		}
	}
}

// 将pinnedAt放到对应的object中。要求objs和pinnedAt都按ObjectID升序
func DetailsFillPinnedAt(objs []ObjectDetail, pinnedAt []cdssdk.PinnedObject) {
	pinnedCur := 0
	for i := range objs {
		obj := &objs[i]
		for ; pinnedCur < len(pinnedAt); pinnedCur++ {
			if pinnedAt[pinnedCur].ObjectID != obj.Object.ObjectID {
				break
			}
			obj.PinnedAt = append(obj.PinnedAt, pinnedAt[pinnedCur].StorageID)
		}
	}
}

type GrouppedObjectBlock struct {
	ObjectID   cdssdk.ObjectID
	Index      int
	FileHash   cdssdk.FileHash
	StorageIDs []cdssdk.StorageID
}

func (o *ObjectDetail) GroupBlocks() []GrouppedObjectBlock {
	grps := make(map[int]GrouppedObjectBlock)
	for _, block := range o.Blocks {
		grp, ok := grps[block.Index]
		if !ok {
			grp = GrouppedObjectBlock{
				ObjectID: block.ObjectID,
				Index:    block.Index,
				FileHash: block.FileHash,
			}
		}
		grp.StorageIDs = append(grp.StorageIDs, block.StorageID)
		grps[block.Index] = grp
	}

	return sort2.Sort(lo.Values(grps), func(l, r GrouppedObjectBlock) int { return l.Index - r.Index })
}

type LocalMachineInfo struct {
	HubID      *cdssdk.HubID     `json:"hubID"`
	ExternalIP string            `json:"externalIP"`
	LocalIP    string            `json:"localIP"`
	LocationID cdssdk.LocationID `json:"locationID"`
}

type PackageAccessStat struct {
	PackageID cdssdk.PackageID `gorm:"column:PackageID; primaryKey; type:bigint" json:"packageID"`
	StorageID cdssdk.StorageID `gorm:"column:StorageID; primaryKey; type:bigint" json:"storageID"`
	Amount    float64          `gorm:"column:Amount; type:double" json:"amount"`   // 前一日的读取量的滑动平均值
	Counter   float64          `gorm:"column:Counter; type:double" json:"counter"` // 当日的读取量
}

func (PackageAccessStat) TableName() string {
	return "PackageAccessStat"
}

type ObjectAccessStat struct {
	ObjectID  cdssdk.ObjectID  `gorm:"column:ObjectID; primaryKey; type:bigint" json:"objectID"`
	StorageID cdssdk.StorageID `gorm:"column:StorageID; primaryKey; type:bigint" json:"storageID"`
	Amount    float64          `gorm:"column:Amount; type:float; not null" json:"amount"`   // 前一日的读取量的滑动平均值
	Counter   float64          `gorm:"column:Counter; type:float; not null" json:"counter"` // 当日的读取量
}

func (ObjectAccessStat) TableName() string {
	return "ObjectAccessStat"
}

type StorageDetail struct {
	Storage   cdssdk.Storage `json:"storage"`
	MasterHub *cdssdk.Hub    `json:"masterHub"`
}

type ObjectStorage struct {
	Manufacturer string `json:"manufacturer"`
	Region       string `json:"region"`
	AK           string `json:"access_key_id"`
	SK           string `json:"secret_access_key"`
	Endpoint     string `json:"endpoint"`
	Bucket       string `json:"bucket"`
}

const (
	HuaweiCloud = "HuaweiCloud"
	AliCloud    = "AliCloud"
	SugonCloud  = "SugonCloud"
)

type LoadedPackageID struct {
	UserID    cdssdk.UserID
	PackageID cdssdk.PackageID
}
