package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type UserService interface {
	CreateUser(msg *CreateUser) (*CreateUserResp, *mq.CodeMessage)

	DeleteUser(msg *DeleteUser) (*DeleteUserResp, *mq.CodeMessage)
}

// 创建用户
var _ = Register(Service.CreateUser)

type CreateUser struct {
	mq.MessageBodyBase
	Name string `json:"name"`
}

type CreateUserResp struct {
	mq.MessageBodyBase
	User cdssdk.User `json:"user"`
}

func ReqCreateUser(name string) *CreateUser {
	return &CreateUser{
		Name: name,
	}
}

func RespCreateUser(user cdssdk.User) *CreateUserResp {
	return &CreateUserResp{
		User: user,
	}
}

func (c *Client) CreateUser(msg *CreateUser) (*CreateUserResp, error) {
	return mq.Request(Service.CreateUser, c.rabbitCli, msg)
}

// 删除用户
var _ = Register(Service.DeleteUser)

type DeleteUser struct {
	mq.MessageBodyBase
	UserID cdssdk.UserID `json:"userID"`
}

type DeleteUserResp struct {
	mq.MessageBodyBase
}

func ReqDeleteUser(userID cdssdk.UserID) *DeleteUser {
	return &DeleteUser{
		UserID: userID,
	}
}

func RespDeleteUser() *DeleteUserResp {
	return &DeleteUserResp{}
}

func (c *Client) DeleteUser(msg *DeleteUser) (*DeleteUserResp, error) {
	return mq.Request(Service.DeleteUser, c.rabbitCli, msg)
}
