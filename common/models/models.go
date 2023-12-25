package stgmod

import (
	"github.com/samber/lo"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type ObjectBlock struct {
	ObjectID cdssdk.ObjectID `db:"ObjectID" json:"objectID"`
	Index    int             `db:"Index" json:"index"`
	NodeID   cdssdk.NodeID   `db:"NodeID" json:"nodeID"` // 这个块应该在哪个节点上
	FileHash string          `db:"FileHash" json:"fileHash"`
}

type ObjectDetail struct {
	Object cdssdk.Object `json:"object"`
	Blocks []ObjectBlock `json:"blocks"`
}

func NewObjectDetail(object cdssdk.Object, blocks []ObjectBlock) ObjectDetail {
	return ObjectDetail{
		Object: object,
		Blocks: blocks,
	}
}

type GrouppedObjectBlock struct {
	ObjectID cdssdk.ObjectID
	Index    int
	FileHash string
	NodeIDs  []cdssdk.NodeID
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
		grp.NodeIDs = append(grp.NodeIDs, block.NodeID)
		grps[block.Index] = grp
	}

	return lo.Values(grps)
}

type LocalMachineInfo struct {
	NodeID     *cdssdk.NodeID    `json:"nodeID"`
	ExternalIP string            `json:"externalIP"`
	LocalIP    string            `json:"localIP"`
	LocationID cdssdk.LocationID `json:"locationID"`
}
