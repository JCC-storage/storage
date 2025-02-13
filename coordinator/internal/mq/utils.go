package mq

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

func getBlockTypeFromRed(red cdssdk.Redundancy) string {
	switch red.(type) {
	case *cdssdk.NoneRedundancy:
		return stgmod.BlockTypeRaw

	case *cdssdk.ECRedundancy:
		return stgmod.BlockTypeEC

	case *cdssdk.LRCRedundancy:
		return stgmod.BlockTypeEC

	case *cdssdk.SegmentRedundancy:
		return stgmod.BlockTypeSegment
	}
	return ""
}
