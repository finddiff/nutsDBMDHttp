package nutshttp

import (
	nutsdb "github.com/finddiff/nutsDBMD"
)

func Enable(db *nutsdb.DB, addr string) error {
	s := NewNutsHTTPServer(db)
	if addr == "" {
		return s.Run(":8080")
	}
	return s.Run(addr)
}
