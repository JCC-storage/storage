package db2

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gorm.io/gorm"
)

type UserDB struct {
	*DB
}

func (db *DB) User() *UserDB {
	return &UserDB{DB: db}
}

func (db *UserDB) GetByID(ctx SQLContext, userID cdssdk.UserID) (cdssdk.User, error) {
	var ret cdssdk.User
	err := ctx.Table("User").Where("UserID = ?", userID).First(&ret).Error
	return ret, err
}

func (db *UserDB) GetByName(ctx SQLContext, name string) (cdssdk.User, error) {
	var ret cdssdk.User
	err := ctx.Table("User").Where("Name = ?", name).First(&ret).Error
	return ret, err
}

func (db *UserDB) Create(ctx SQLContext, name string) (cdssdk.User, error) {
	_, err := db.GetByName(ctx, name)
	if err == nil {
		return cdssdk.User{}, gorm.ErrDuplicatedKey
	}
	if err != gorm.ErrRecordNotFound {
		return cdssdk.User{}, err
	}

	user := cdssdk.User{Name: name}
	err = ctx.Table("User").Create(&user).Error
	return user, err
}

func (*UserDB) Delete(ctx SQLContext, userID cdssdk.UserID) error {
	return ctx.Table("User").Delete(&cdssdk.User{UserID: userID}).Error
}
