package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
)

type UserService struct {
	*Server
}

func (s *Server) User() *UserService {
	return &UserService{
		Server: s,
	}
}

func (s *UserService) Create(ctx *gin.Context) {
	log := logger.WithField("HTTP", "User.Create")
	var req cdsapi.UserCreate
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	user, err := s.svc.UserSvc().Create(req.Name)
	if err != nil {
		log.Warnf("create user: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, err.Error()))
		return
	}

	ctx.JSON(http.StatusOK, OK(user))
}

func (s *UserService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "User.Delete")
	var req cdsapi.UserDelete
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	if err := s.svc.UserSvc().Delete(req.UserID); err != nil {
		log.Warnf("delete user: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, err.Error()))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}
