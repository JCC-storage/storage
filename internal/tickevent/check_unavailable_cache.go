package tickevent

import (
	"gitlink.org.cn/cloudream/common/utils/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

type CheckUnavailableCache struct {
}

func NewCheckUnavailableCache() *CheckUnavailableCache {
	return &CheckUnavailableCache{}
}

func (e *CheckUnavailableCache) Execute(ctx ExecuteContext) {
	nodes, err := mysql.Node.GetAllNodes(ctx.Args.DB.SQLCtx())
	if err != nil {
		logger.Warnf("get all nodes failed, err: %s", err.Error())
		return
	}

	for _, node := range nodes {
		ctx.Args.EventExecutor.Post(event.NewCheckUnavailableCache(node.NodeID))
	}
}
