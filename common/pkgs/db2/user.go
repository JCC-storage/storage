package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type UserDB struct {
	*DB
}

func (db *DB) User() *UserDB {
	return &UserDB{DB: db}
}

func (db *UserDB) GetByID(ctx SQLContext, userID cdssdk.UserID) (model.User, error) {
	var ret model.User
	err := ctx.Table("User").Where("UserID = ?", userID).First(&ret).Error
	return ret, err
}
