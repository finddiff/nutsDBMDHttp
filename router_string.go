package nutshttp

func (s *NutsHTTPServer) initStringRouter() {
	sr := s.r.Group("/string")

	sr.GET("get/:bucket/:key", s.Get)

	sr.POST("update/:bucket/:key", s.Update)

	sr.POST("batchupdate/:bucket", s.BatchUpdate)

	sr.POST("swaps/:bucket/:key", s.Swaps)

	sr.POST("delete/:bucket/:key", s.Delete)

	sr.GET("scan/:bucket/:scanType", s.Scan)

	sr.GET("removefiles/:count", s.DeleteOldFiles)

	sr.GET("delivaldallfiles/", s.DeleteAllOldFiles)
}
