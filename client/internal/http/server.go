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
	s.engine.GET(cdssdk.ObjectDownloadPath, s.Object().Download)
	s.engine.POST(cdssdk.ObjectUploadPath, s.Object().Upload)
	s.engine.GET(cdssdk.ObjectGetPackageObjectsPath, s.Object().GetPackageObjects)
	s.engine.POST(cdssdk.ObjectUpdateInfoPath, s.Object().UpdateInfo)
	s.engine.POST(cdssdk.ObjectMovePath, s.Object().Move)
	s.engine.POST(cdssdk.ObjectDeletePath, s.Object().Delete)

	s.engine.GET(cdssdk.PackageGetPath, s.Package().Get)
	s.engine.GET(cdssdk.PackageGetByNamePath, s.Package().GetByName)
	s.engine.POST(cdssdk.PackageCreatePath, s.Package().Create)
	s.engine.POST(cdssdk.PackageDeletePath, s.Package().Delete)
	s.engine.GET(cdssdk.PackageListBucketPackagesPath, s.Package().ListBucketPackages)
	s.engine.GET(cdssdk.PackageGetCachedNodesPath, s.Package().GetCachedNodes)
	s.engine.GET(cdssdk.PackageGetLoadedNodesPath, s.Package().GetLoadedNodes)

	// 存储管理相关路由配置
	s.engine.POST("/storage/loadPackage", s.Storage().LoadPackage)     // 处理加载包请求
	s.engine.POST("/storage/createPackage", s.Storage().CreatePackage) // 处理创建包请求
	s.engine.GET("/storage/getInfo", s.Storage().GetInfo)              // 处理获取存储信息请求

	// 缓存管理相关路由配置
	s.engine.POST(cdssdk.CacheMovePackagePath, s.Cache().MovePackage) // 处理移动包到缓存请求

	s.engine.GET(cdssdk.BucketGetByNamePath, s.Bucket().GetByName)
	s.engine.POST(cdssdk.BucketCreatePath, s.Bucket().Create)
	s.engine.POST(cdssdk.BucketDeletePath, s.Bucket().Delete)
	s.engine.GET(cdssdk.BucketListUserBucketsPath, s.Bucket().ListUserBuckets)
}
