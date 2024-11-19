package http

import (
	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	"gitlink.org.cn/cloudream/storage/client/internal/services"
)

type Server struct {
	engine     *gin.Engine
	listenAddr string
	svc        *services.Service
}

func NewServer(listenAddr string, svc *services.Service) (*Server, error) {
	engine := gin.New()

	return &Server{
		engine:     engine,
		listenAddr: listenAddr,
		svc:        svc,
	}, nil
}

func (s *Server) Serve() error {
	s.initRouters()

	logger.Infof("start serving http at: %s", s.listenAddr)
	err := s.engine.Run(s.listenAddr)

	if err != nil {
		logger.Infof("http stopped with error: %s", err.Error())
		return err
	}

	logger.Infof("http stopped")
	return nil
}

func (s *Server) initRouters() {
	rt := s.engine.Use()

	// initTemp(rt, s)

	rt.GET(cdsapi.ObjectDownloadPath, s.Object().Download)
	rt.POST(cdsapi.ObjectUploadPath, s.Object().Upload)
	rt.GET(cdsapi.ObjectGetPackageObjectsPath, s.Object().GetPackageObjects)
	rt.POST(cdsapi.ObjectUpdateInfoPath, s.Object().UpdateInfo)
	rt.POST(cdsapi.ObjectMovePath, s.Object().Move)
	rt.POST(cdsapi.ObjectDeletePath, s.Object().Delete)

	rt.GET(cdsapi.PackageGetPath, s.Package().Get)
	rt.GET(cdsapi.PackageGetByNamePath, s.Package().GetByName)
	rt.POST(cdsapi.PackageCreatePath, s.Package().Create)
	rt.POST(cdsapi.PackageCreateLoadPath, s.Package().CreateLoad)
	rt.POST(cdsapi.PackageDeletePath, s.Package().Delete)
	rt.GET(cdsapi.PackageListBucketPackagesPath, s.Package().ListBucketPackages)
	rt.GET(cdsapi.PackageGetCachedStoragesPath, s.Package().GetCachedStorages)
	rt.GET(cdsapi.PackageGetLoadedStoragesPath, s.Package().GetLoadedStorages)

	rt.POST(cdsapi.StorageLoadPackagePath, s.Storage().LoadPackage)
	rt.POST(cdsapi.StorageCreatePackagePath, s.Storage().CreatePackage)
	rt.GET(cdsapi.StorageGetPath, s.Storage().Get)

	rt.POST(cdsapi.CacheMovePackagePath, s.Cache().MovePackage)

	rt.GET(cdsapi.BucketGetByNamePath, s.Bucket().GetByName)
	rt.POST(cdsapi.BucketCreatePath, s.Bucket().Create)
	rt.POST(cdsapi.BucketDeletePath, s.Bucket().Delete)
	rt.GET(cdsapi.BucketListUserBucketsPath, s.Bucket().ListUserBuckets)
}
