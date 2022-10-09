package nutshttp

import (
	"net/http"

	nutsdb "github.com/finddiff/nutsDBMD"
	"github.com/gin-gonic/gin"
)

type NutsHTTPServer struct {
	core *core
	r    *gin.Engine
}

func NewNutsHTTPServer(db *nutsdb.DB) *NutsHTTPServer {
	c := &core{db}

	r := gin.Default()

	s := &NutsHTTPServer{
		core: c,
		r:    r,
	}

	s.initRouter()

	return s
}

func (s *NutsHTTPServer) Run(addr string) error {
	return http.ListenAndServe(addr, s.r)
}

func (s *NutsHTTPServer) initRouter() {
	s.initSetRouter()

	s.initListRouter()

	s.initStringRouter()

	s.initZSetRouter()
}
