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

	s.routeV1(s.engine)

	rt.GET(cdsapi.ObjectListPathByPath, s.Object().ListByPath)
	rt.POST(cdsapi.ObjectListByIDsPath, s.Object().ListByIDs)
	rt.GET(cdsapi.ObjectDownloadPath, s.Object().Download)
	rt.GET(cdsapi.ObjectDownloadByPathPath, s.Object().DownloadByPath)
	rt.POST(cdsapi.ObjectUploadPath, s.Object().Upload)
	rt.GET(cdsapi.ObjectGetPackageObjectsPath, s.Object().GetPackageObjects)
	rt.POST(cdsapi.ObjectUpdateInfoPath, s.Object().UpdateInfo)
	rt.POST(cdsapi.ObjectUpdateInfoByPathPath, s.Object().UpdateInfoByPath)
	rt.POST(cdsapi.ObjectMovePath, s.Object().Move)
	rt.POST(cdsapi.ObjectDeletePath, s.Object().Delete)
	rt.POST(cdsapi.ObjectDeleteByPathPath, s.Object().DeleteByPath)
	rt.POST(cdsapi.ObjectClonePath, s.Object().Clone)

	rt.GET(cdsapi.PackageGetPath, s.Package().Get)
	rt.GET(cdsapi.PackageGetByFullNamePath, s.Package().GetByFullName)
	rt.POST(cdsapi.PackageCreatePath, s.Package().Create)
	rt.POST(cdsapi.PackageCreateLoadPath, s.Package().CreateLoad)
	rt.POST(cdsapi.PackageDeletePath, s.Package().Delete)
	rt.POST(cdsapi.PackageClonePath, s.Package().Clone)
	rt.GET(cdsapi.PackageListBucketPackagesPath, s.Package().ListBucketPackages)
	rt.GET(cdsapi.PackageGetCachedStoragesPath, s.Package().GetCachedStorages)

	rt.POST(cdsapi.StorageLoadPackagePath, s.Storage().LoadPackage)
	rt.POST(cdsapi.StorageCreatePackagePath, s.Storage().CreatePackage)
	rt.GET(cdsapi.StorageGetPath, s.Storage().Get)

	rt.POST(cdsapi.CacheMovePackagePath, s.Cache().MovePackage)

	rt.GET(cdsapi.BucketGetByNamePath, s.Bucket().GetByName)
	rt.POST(cdsapi.BucketCreatePath, s.Bucket().Create)
	rt.POST(cdsapi.BucketDeletePath, s.Bucket().Delete)
	rt.GET(cdsapi.BucketListUserBucketsPath, s.Bucket().ListUserBuckets)

}

func (s *Server) routeV1(eg *gin.Engine) {
	v1 := eg.Group("/v1")

	v1.GET(cdsapi.ObjectListPathByPath, s.Object().ListByPath)
	v1.POST(cdsapi.ObjectListByIDsPath, s.Object().ListByIDs)
	v1.GET(cdsapi.ObjectDownloadPath, s.Object().Download)
	v1.GET(cdsapi.ObjectDownloadByPathPath, s.Object().DownloadByPath)
	v1.POST(cdsapi.ObjectUploadPath, s.Object().Upload)
	v1.GET(cdsapi.ObjectGetPackageObjectsPath, s.Object().GetPackageObjects)
	v1.POST(cdsapi.ObjectUpdateInfoPath, s.Object().UpdateInfo)
	v1.POST(cdsapi.ObjectUpdateInfoByPathPath, s.Object().UpdateInfoByPath)
	v1.POST(cdsapi.ObjectMovePath, s.Object().Move)
	v1.POST(cdsapi.ObjectDeletePath, s.Object().Delete)
	v1.POST(cdsapi.ObjectDeleteByPathPath, s.Object().DeleteByPath)
	v1.POST(cdsapi.ObjectClonePath, s.Object().Clone)

	v1.GET(cdsapi.PackageGetPath, s.Package().Get)
	v1.GET(cdsapi.PackageGetByFullNamePath, s.Package().GetByFullName)
	v1.POST(cdsapi.PackageCreatePath, s.Package().Create)
	v1.POST(cdsapi.PackageCreateLoadPath, s.Package().CreateLoad)
	v1.POST(cdsapi.PackageDeletePath, s.Package().Delete)
	v1.POST(cdsapi.PackageClonePath, s.Package().Clone)
	v1.GET(cdsapi.PackageListBucketPackagesPath, s.Package().ListBucketPackages)
	v1.GET(cdsapi.PackageGetCachedStoragesPath, s.Package().GetCachedStorages)

	v1.POST(cdsapi.StorageLoadPackagePath, s.Storage().LoadPackage)
	v1.POST(cdsapi.StorageCreatePackagePath, s.Storage().CreatePackage)
	v1.GET(cdsapi.StorageGetPath, s.Storage().Get)

	v1.POST(cdsapi.CacheMovePackagePath, s.Cache().MovePackage)

	v1.GET(cdsapi.BucketGetByNamePath, s.Bucket().GetByName)
	v1.POST(cdsapi.BucketCreatePath, s.Bucket().Create)
	v1.POST(cdsapi.BucketDeletePath, s.Bucket().Delete)
	v1.GET(cdsapi.BucketListUserBucketsPath, s.Bucket().ListUserBuckets)
}
