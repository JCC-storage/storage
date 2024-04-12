package services

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

// BucketService 是对存储桶进行操作的服务类
type BucketService struct {
	*Service
}

// BucketSvc 创建并返回一个BucketService实例
func (svc *Service) BucketSvc() *BucketService {
	return &BucketService{Service: svc}
}

// GetBucket 根据用户ID和桶ID获取桶信息
// userID: 用户的唯一标识
// bucketID: 桶的唯一标识
// 返回值: 桶的信息和可能发生的错误
func (svc *BucketService) GetBucket(userID cdssdk.UserID, bucketID cdssdk.BucketID) (model.Bucket, error) {
	// TODO: 此函数尚未实现
	panic("not implement yet")
}

func (svc *BucketService) GetBucketByName(userID cdssdk.UserID, bucketName string) (model.Bucket, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return model.Bucket{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.GetBucketByName(coormq.ReqGetBucketByName(userID, bucketName))
	if err != nil {
		return model.Bucket{}, fmt.Errorf("get bucket by name failed, err: %w", err)
	}

	return resp.Bucket, nil
}

func (svc *BucketService) GetUserBuckets(userID cdssdk.UserID) ([]model.Bucket, error) {
	// 从CoordinatorMQPool中获取Coordinator客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli) // 确保客户端被释放

	// 向Coordinator发送请求获取用户桶信息
	resp, err := coorCli.GetUserBuckets(coormq.NewGetUserBuckets(userID))
	if err != nil {
		return nil, fmt.Errorf("get user buckets failed, err: %w", err)
	}

	return resp.Buckets, nil
}

// GetBucketPackages 获取指定用户和桶的所有包
// userID: 用户的唯一标识
// bucketID: 桶的唯一标识
// 返回值: 桶的所有包列表和可能发生的错误
func (svc *BucketService) GetBucketPackages(userID cdssdk.UserID, bucketID cdssdk.BucketID) ([]model.Package, error) {
	// 获取Coordinator客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli) // 确保客户端被释放

	// 请求Coordinator获取指定桶的包信息
	resp, err := coorCli.GetBucketPackages(coormq.NewGetBucketPackages(userID, bucketID))
	if err != nil {
		return nil, fmt.Errorf("get bucket packages failed, err: %w", err)
	}

	return resp.Packages, nil
}

func (svc *BucketService) CreateBucket(userID cdssdk.UserID, bucketName string) (cdssdk.Bucket, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.Bucket{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli) // 确保客户端被释放

	// 请求Coordinator创建新桶
	resp, err := coorCli.CreateBucket(coormq.NewCreateBucket(userID, bucketName))
	if err != nil {
		return cdssdk.Bucket{}, fmt.Errorf("creating bucket: %w", err)
	}

	return resp.Bucket, nil
}

// DeleteBucket 删除指定的桶
// userID: 用户的唯一标识
// bucketID: 桶的唯一标识
// 返回值: 可能发生的错误
func (svc *BucketService) DeleteBucket(userID cdssdk.UserID, bucketID cdssdk.BucketID) error {
	// 获取Coordinator客户端
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli) // 确保客户端被释放

	_, err = coorCli.DeleteBucket(coormq.NewDeleteBucket(userID, bucketID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}

	return nil
}
