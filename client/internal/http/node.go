package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// NodeService 结构体代表了节点服务，它包含了一个Server实例。
type NodeService struct {
	*Server
}

// NodeSvc 为Server结构体提供一个方法，返回一个NodeService的实例。
// 这个方法主要用于在Server实例中访问NodeService。
func (s *Server) NodeSvc() *NodeService {
	return &NodeService{
		Server: s,
	}
}

// GetNodesReq 结构体定义了获取节点信息请求的参数。
// 它包含一个NodeIDs字段，该字段是需要查询的节点的ID列表，是必需的。
type GetNodesReq struct {
	NodeIDs *[]cdssdk.NodeID `form:"nodeIDs" binding:"required"`
}

// GetNodesResp 结构体与cdssdk包中的NodeGetNodesResp类型相同，用于定义获取节点信息的响应。
type GetNodesResp = cdssdk.NodeGetNodesResp

// GetNodes 是一个处理获取节点信息请求的方法。
// 它使用Gin框架的Context来处理HTTP请求，获取请求参数，并返回节点信息。
// ctx *gin.Context: 代表当前的HTTP请求上下文。
func (s *ObjectService) GetNodes(ctx *gin.Context) {
	// 初始化日志记录器，添加"HTTP"字段标识。
	log := logger.WithField("HTTP", "Node.GetNodes")

	var req GetNodesReq
	// 尝试绑定查询参数到请求结构体，如果出错则返回错误信息。
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		// 参数绑定失败，返回400状态码和错误信息。
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	// 调用NodeSvc获取节点信息，如果出错则返回操作失败的错误信息。
	nodes, err := s.svc.NodeSvc().GetNodes(*req.NodeIDs)
	if err != nil {
		log.Warnf("getting nodes: %s", err.Error())
		// 获取节点信息失败，返回操作失败的错误信息。
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get nodes failed"))
		return
	}

	// 节点信息获取成功，返回200状态码和节点信息。
	ctx.JSON(http.StatusOK, OK(GetNodesResp{Nodes: nodes}))
}
