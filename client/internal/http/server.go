package http

import (
	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/client/internal/services"
)

// Server 结构体定义了HTTP服务的基本配置和操作
type Server struct {
	engine     *gin.Engine       // Gin框架的HTTP引擎
	listenAddr string            // 服务监听地址
	svc        *services.Service // 业务逻辑服务实例
}

// NewServer 创建一个新的Server实例
// listenAddr: 服务监听的地址
// svc: 用于处理HTTP请求的业务逻辑服务实例
// 返回值: 初始化好的Server实例和可能发生的错误
func NewServer(listenAddr string, svc *services.Service) (*Server, error) {
	engine := gin.New()

	return &Server{
		engine:     engine,
		listenAddr: listenAddr,
		svc:        svc,
	}, nil
}

// Serve 启动HTTP服务并监听请求
// 返回值: 服务停止时可能发生的错误
func (s *Server) Serve() error {
	s.initRouters() // 初始化路由

	logger.Infof("start serving http at: %s", s.listenAddr)
	err := s.engine.Run(s.listenAddr)

	if err != nil {
		logger.Infof("http stopped with error: %s", err.Error())
		return err
	}

	logger.Infof("http stopped")
	return nil
}

// initRouters 初始化所有HTTP请求的路由
//
// 它主要用于配置和初始化与HTTP请求相关的所有路由，
// 包括对象存储、包管理、存储管理、缓存管理和存储桶管理等。
func (s *Server) initRouters() {
	// 对象存储相关路由配置
	s.engine.GET(cdssdk.ObjectDownloadPath, s.Object().Download)                   // 处理对象下载请求
	s.engine.POST(cdssdk.ObjectUploadPath, s.Object().Upload)                      // 处理对象上传请求
	s.engine.GET(cdssdk.ObjectGetPackageObjectsPath, s.Object().GetPackageObjects) // 处理获取包内对象请求

	// 包管理相关路由配置
	s.engine.GET(cdssdk.PackageGetPath, s.Package().Get)                // 处理获取包信息请求
	s.engine.POST(cdssdk.PackageCreatePath, s.Package().Create)         // 处理创建包请求
	s.engine.POST("/package/delete", s.Package().Delete)                // 处理删除包请求
	s.engine.GET("/package/getCachedNodes", s.Package().GetCachedNodes) // 处理获取缓存节点请求
	s.engine.GET("/package/getLoadedNodes", s.Package().GetLoadedNodes) // 处理获取已加载节点请求

	// 存储管理相关路由配置
	s.engine.POST("/storage/loadPackage", s.Storage().LoadPackage)     // 处理加载包请求
	s.engine.POST("/storage/createPackage", s.Storage().CreatePackage) // 处理创建包请求
	s.engine.GET("/storage/getInfo", s.Storage().GetInfo)              // 处理获取存储信息请求

	// 缓存管理相关路由配置
	s.engine.POST(cdssdk.CacheMovePackagePath, s.Cache().MovePackage) // 处理移动包到缓存请求

	// 存储桶管理相关路由配置
	s.engine.POST(cdssdk.BucketCreatePath, s.Bucket().Create) // 处理创建存储桶请求
	s.engine.POST(cdssdk.BucketDeletePath, s.Bucket().Delete) // 处理删除存储桶请求
}
