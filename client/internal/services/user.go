package services

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type UserService struct {
	*Service
}

func (svc *Service) UserSvc() *UserService {
	return &UserService{Service: svc}
}

func (svc *UserService) Create(name string) (cdssdk.User, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.User{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.CreateUser(coormq.ReqCreateUser(name))
	if err != nil {
		return cdssdk.User{}, err
	}

	return resp.User, nil
}

func (svc *UserService) Delete(userID cdssdk.UserID) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	_, err = coorCli.DeleteUser(coormq.ReqDeleteUser(userID))
	return err
}
