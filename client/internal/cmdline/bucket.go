package cmdline

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// BucketListUserBuckets 列出指定用户的存储桶列表。
// ctx: 命令上下文，提供必要的服务和配置。
// 返回值: 执行错误时返回error。
func BucketListUserBuckets(ctx CommandContext) error {
	userID := cdssdk.UserID(1)

	// 获取指定用户ID的存储桶列表
	buckets, err := ctx.Cmdline.Svc.BucketSvc().GetUserBuckets(userID)
	if err != nil {
		return err
	}

	// 打印找到的存储桶数量和用户ID
	fmt.Printf("Find %d buckets for user %d:\n", len(buckets), userID)

	// 构建存储桶列表的表格显示
	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "CreatorID"})

	for _, bucket := range buckets {
		tb.AppendRow(table.Row{bucket.BucketID, bucket.Name, bucket.CreatorID})
	}

	// 打印存储桶列表表格
	fmt.Print(tb.Render())
	return nil
}

// BucketCreateBucket 为指定用户创建一个新的存储桶。
// ctx: 命令上下文，提供必要的服务和配置。
// bucketName: 新存储桶的名称。
// 返回值: 执行错误时返回error。
func BucketCreateBucket(ctx CommandContext, bucketName string) error {
	userID := cdssdk.UserID(1)

	// 创建存储桶并获取新存储桶的ID
	bucketID, err := ctx.Cmdline.Svc.BucketSvc().CreateBucket(userID, bucketName)
	if err != nil {
		return err
	}

	// 打印创建存储桶成功的消息
	fmt.Printf("Create bucket %s success, id: %d", bucketName, bucketID)
	return nil
}

// BucketDeleteBucket 删除指定的存储桶。
// ctx: 命令上下文，提供必要的服务和配置。
// bucketID: 要删除的存储桶ID。
// 返回值: 执行错误时返回error。
func BucketDeleteBucket(ctx CommandContext, bucketID cdssdk.BucketID) error {
	userID := cdssdk.UserID(1)

	// 删除指定的存储桶
	err := ctx.Cmdline.Svc.BucketSvc().DeleteBucket(userID, bucketID)
	if err != nil {
		return err
	}

	// 打印删除成功的消息
	fmt.Printf("Delete bucket %d success ", bucketID)
	return nil
}

// 初始化命令注册
func init() {
	// 注册列出用户存储桶的命令
	commands.MustAdd(BucketListUserBuckets, "bucket", "ls")

	// 注册创建存储桶的命令
	commands.MustAdd(BucketCreateBucket, "bucket", "new")

	// 注册删除存储桶的命令
	commands.MustAdd(BucketDeleteBucket, "bucket", "delete")
}
