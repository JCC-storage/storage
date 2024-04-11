package stgglb

import (
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
)

var Local *stgmodels.LocalMachineInfo

// InitLocal
//
//	@Description: 初始化本地机器信息
//	@param info
func InitLocal(info *stgmodels.LocalMachineInfo) {
	Local = info
}
