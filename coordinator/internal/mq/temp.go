package mq

import (
	"fmt"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetDatabaseAll(msg *coormq.GetDatabaseAll) (*coormq.GetDatabaseAllResp, *mq.CodeMessage) {
	var bkts []cdssdk.Bucket
	var pkgs []cdssdk.Package
	var objs []stgmod.ObjectDetail

	err := svc.db2.DoTx(func(tx db2.SQLContext) error {
		var err error
		bkts, err = svc.db2.Bucket().GetUserBuckets(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("get user buckets: %w", err)
		}

		for _, bkt := range bkts {
			ps, err := svc.db2.Package().GetBucketPackages(tx, msg.UserID, bkt.BucketID)
			if err != nil {
				return fmt.Errorf("get bucket packages: %w", err)
			}
			pkgs = append(pkgs, ps...)
		}

		for _, pkg := range pkgs {
			os, err := svc.db2.Object().GetPackageObjectDetails(tx, pkg.PackageID)
			if err != nil {
				return fmt.Errorf("get package object details: %w", err)
			}
			objs = append(objs, os...)
		}

		return nil
	})
	if err != nil {
		logger.Warnf("batch deleting objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch delete objects failed")
	}

	return mq.ReplyOK(coormq.RespGetDatabaseAll(bkts, pkgs, objs))
}
