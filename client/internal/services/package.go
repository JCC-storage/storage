package services

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// PackageService 提供对包相关操作的服务接口
type PackageService struct {
	*Service
}

// PackageSvc 创建并返回一个PackageService的实例
func (svc *Service) PackageSvc() *PackageService {
	return &PackageService{Service: svc}
}

func (svc *PackageService) Get(userID cdssdk.UserID, packageID cdssdk.PackageID) (*cdssdk.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器请求获取包信息
	getResp, err := coorCli.GetPackage(coormq.NewGetPackage(userID, packageID))
	if err != nil {
		return nil, err
	}

	return &getResp.Package, nil
}

func (svc *PackageService) GetByName(userID cdssdk.UserID, bucketName string, packageName string) (*cdssdk.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetPackageByName(coormq.ReqGetPackageByName(userID, bucketName, packageName))
	if err != nil {
		// TODO 要附加日志信息，但不能直接%w，因为外部需要判断错误吗
		return nil, err
	}

	return &getResp.Package, nil
}

func (svc *PackageService) GetBucketPackages(userID cdssdk.UserID, bucketID cdssdk.BucketID) ([]cdssdk.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetBucketPackages(coormq.NewGetBucketPackages(userID, bucketID))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return getResp.Packages, nil
}

func (svc *PackageService) Create(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string) (cdssdk.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.Package{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器发送创建包的请求
	resp, err := coorCli.CreatePackage(coormq.NewCreatePackage(userID, bucketID, name))
	if err != nil {
		return cdssdk.Package{}, err
	}

	return resp.Package, nil
}

func (svc *PackageService) DownloadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID) (downloader.DownloadIterator, error) {
	// TODO 检查用户ID
	return svc.Downloader.DownloadPackage(packageID), nil
}

// DeletePackage 删除指定的包
func (svc *PackageService) DeletePackage(userID cdssdk.UserID, packageID cdssdk.PackageID) error {
	// 从协调器MQ池中获取客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器发送删除包的请求
	_, err = coorCli.DeletePackage(coormq.NewDeletePackage(userID, packageID))
	if err != nil {
		return fmt.Errorf("deleting package: %w", err)
	}

	return nil
}

// GetCachedStorages 获取指定包的缓存节点信息
func (svc *PackageService) GetCachedStorages(userID cdssdk.UserID, packageID cdssdk.PackageID) (cdssdk.PackageCachingInfo, error) {
	// 从协调器MQ池中获取客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.PackageCachingInfo{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器请求获取包的缓存节点信息
	resp, err := coorCli.GetPackageCachedStorages(coormq.ReqGetPackageCachedStorages(userID, packageID))
	if err != nil {
		return cdssdk.PackageCachingInfo{}, fmt.Errorf("get package cached storages: %w", err)
	}

	// 构造并返回缓存信息
	tmp := cdssdk.PackageCachingInfo{
		StorageInfos: resp.StorageInfos,
		PackageSize:  resp.PackageSize,
	}
	return tmp, nil
}

// GetLoadedStorages 获取指定包加载的节点列表
func (svc *PackageService) GetLoadedStorages(userID cdssdk.UserID, packageID cdssdk.PackageID) ([]cdssdk.StorageID, error) {
	// 从协调器MQ池中获取客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 向协调器请求获取加载指定包的节点ID列表
	resp, err := coorCli.GetPackageLoadedStorages(coormq.ReqGetPackageLoadedStorages(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("get package loaded storages: %w", err)
	}
	return resp.StorageIDs, nil
}
