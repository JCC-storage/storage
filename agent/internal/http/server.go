package http

import (
	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
)

type Server struct {
	engine     *gin.Engine
	listenAddr string
	svc        *Service
}

func NewServer(listenAddr string, svc *Service) (*Server, error) {
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
	s.engine.GET(cdsapi.GetStreamPath, s.IOSvc().GetStream)
	s.engine.POST(cdsapi.SendStreamPath, s.IOSvc().SendStream)
	s.engine.POST(cdsapi.ExecuteIOPlanPath, s.IOSvc().ExecuteIOPlan)
	s.engine.POST(cdsapi.SendVarPath, s.IOSvc().SendVar)
	s.engine.GET(cdsapi.GetVarPath, s.IOSvc().GetVar)
}
