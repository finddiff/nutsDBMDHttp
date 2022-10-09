package nutshttp

import (
	nutsdb "github.com/finddiff/nutsDBMD"
	"github.com/gin-gonic/gin"
)

func (s *NutsHTTPServer) Get(c *gin.Context) {
	var (
		err     error
		baseUri BaseUri
	)

	if err = c.ShouldBindUri(&baseUri); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	value, err := s.core.Get(baseUri.Bucket, baseUri.Key)
	if err != nil {
		switch err {
		case nutsdb.ErrNotFoundKey:
			WriteError(c, ErrKeyNotFoundInBucket)
		default:
			WriteError(c, APIMessage{Code: 40404, Message: err.Error()})
		}
		return
	}

	WriteSucc(c, value)

}

func (s *NutsHTTPServer) Update(c *gin.Context) {
	type UpdateStringRequest struct {
		Value string `json:"value" binding:"required"`
		Ttl   uint32 `json:"ttl"`
	}
	var (
		err                 error
		baseUri             BaseUri
		updateStringRequest UpdateStringRequest
	)

	if err = c.ShouldBindUri(&baseUri); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	if err = c.ShouldBindJSON(&updateStringRequest); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	err = s.core.Update(baseUri.Bucket, baseUri.Key, updateStringRequest.Value, updateStringRequest.Ttl)
	if err != nil {
		switch err {
		case nutsdb.ErrNotFoundKey:
			WriteError(c, ErrKeyNotFoundInBucket)
		default:
			WriteError(c, ErrUnknown)
		}
		return
	}
	WriteSucc(c, struct{}{})
}

func (s *NutsHTTPServer) BatchUpdate(c *gin.Context) {
	type BucketUri struct {
		Bucket string `uri:"bucket" binding:"required"`
	}

	type UpdateStringRequest struct {
		Items []BatchItem `json:"items" binding:"required"`
	}

	var (
		err error
		//errString           string
		bucketUri           BucketUri
		updateStringRequest UpdateStringRequest
	)

	if err = c.ShouldBindUri(&bucketUri); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	if err = c.ShouldBindJSON(&updateStringRequest); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	if err = s.core.BatchUpdate(bucketUri.Bucket, updateStringRequest.Items); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}
	//errString = ""
	//for _, item := range updateStringRequest.Items {
	//	err = s.core.Update(bucketUri.Bucket, item.Key, item.Value, item.Ttl)
	//	if err != nil {
	//		switch err {
	//		case nutsdb.ErrNotFoundKey:
	//			errString += "Key Not Found In Bucket:" + bucketUri.Bucket + " key:" + item.Key + " value:" + item.Value + " ttl:" + strconv.FormatUint(uint64(item.Ttl), 10) + "\n"
	//			//WriteError(c, ErrKeyNotFoundInBucket)
	//		default:
	//			errString += "KErrUnknown Bucket:" + bucketUri.Bucket + " key:" + item.Key + " value:" + item.Value + " ttl:" + strconv.FormatUint(uint64(item.Ttl), 10) + "\n"
	//			//WriteError(c, ErrUnknown)
	//		}
	//		//return
	//	}
	//}
	//if errString != "" {
	//	WriteError(c, APIMessage{
	//		Message: errString,
	//	})
	//	return
	//}

	WriteSucc(c, struct{}{})
}

func (s *NutsHTTPServer) Swaps(c *gin.Context) {
	type UpdateStringRequest struct {
		OldValue string `json:"oldvalue" binding:"required"`
		Value    string `json:"value" binding:"required"`
		Ttl      uint32 `json:"ttl"`
	}
	var (
		err                 error
		baseUri             BaseUri
		updateStringRequest UpdateStringRequest
	)

	if err = c.ShouldBindUri(&baseUri); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	if err = c.ShouldBindJSON(&updateStringRequest); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}
	//fmt.Printf("oldValue:%v, Value:%v, Ttl:%v\n", updateStringRequest.OldValue, updateStringRequest.Value, updateStringRequest.Ttl)
	err = s.core.Swaps(baseUri.Bucket, baseUri.Key, updateStringRequest.OldValue, updateStringRequest.Value, updateStringRequest.Ttl)
	if err != nil {
		switch err {
		case nutsdb.ErrNotFoundKey:
			WriteError(c, ErrKeyNotFoundInBucket)
		default:
			WriteError(c, ErrUnknown)
		}
		return
	}
	WriteSucc(c, struct{}{})
}

func (s *NutsHTTPServer) Delete(c *gin.Context) {
	var (
		err     error
		baseUri BaseUri
	)

	if err = c.ShouldBindUri(&baseUri); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	_, err = s.core.Get(baseUri.Bucket, baseUri.Key)
	if err != nil {
		switch err {
		case nutsdb.ErrNotFoundKey:
			WriteError(c, ErrKeyNotFoundInBucket)
		default:
			WriteError(c, ErrUnknown)
		}
		return
	}

	err = s.core.Delete(baseUri.Bucket, baseUri.Key)

	if err != nil {
		switch err {
		case nutsdb.ErrKeyEmpty:
			WriteError(c, ErrKeyNotFoundInBucket)
		default:
			WriteError(c, ErrUnknown)
		}
		return
	}
	WriteSucc(c, struct{}{})
}

func (s *NutsHTTPServer) Scan(c *gin.Context) {
	const (
		PrefixScan       = "prefixScan"
		PrefixSearchScan = "prefixSearchScan"
		RangeScan        = "rangeScan"
		GetAll           = "getAll"
	)

	type ScanParam struct {
		Bucket   string `uri:"bucket" binding:"required"`
		ScanType string `uri:"scanType" binding:"required"`
	}

	var (
		err       error
		entries   nutsdb.Entries
		scanParam ScanParam
	)

	if err = c.ShouldBindUri(&scanParam); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	switch scanParam.ScanType {
	case PrefixScan:
		type ScanRequest struct {
			OffSet   *int    `json:"offSet"  binding:"required"`
			LimitNum *int    `json:"limitNum"  binding:"required"`
			Prefix   *string `json:"prefix" binding:"required"`
		}

		var scanReq ScanRequest
		if err = c.ShouldBindJSON(&scanReq); err != nil {
			WriteError(c, APIMessage{
				Message: err.Error(),
			})
			return
		}
		entries, err = s.core.PrefixScan(scanParam.Bucket, *scanReq.Prefix, *scanReq.OffSet, *scanReq.LimitNum)
		if err != nil {
			switch err {
			case nutsdb.ErrPrefixScan:
				WriteError(c, ErrPrefixScan)
			default:
				WriteError(c, ErrUnknown)
			}
			return
		}
		var res = map[string]string{}
		for _, e := range entries {
			res[string(e.Key)] = string(e.Value)
		}
		WriteSucc(c, res)
	case PrefixSearchScan:
		type ScanSearchReq struct {
			OffSet   *int    `json:"offSet"  binding:"required"`
			LimitNum *int    `json:"limitNum"  binding:"required"`
			Prefix   *string `json:"prefix" binding:"required"`
			Reg      *string `json:"reg" binding:"required"`
		}
		var scanSearchReq ScanSearchReq
		if err = c.ShouldBindJSON(&scanSearchReq); err != nil {
			WriteError(c, APIMessage{
				Message: err.Error(),
			})
			return
		}
		entries, err = s.core.PrefixSearchScan(scanParam.Bucket, *scanSearchReq.Prefix, *scanSearchReq.Reg, *scanSearchReq.OffSet, *scanSearchReq.LimitNum)
		if err != nil {
			switch err {
			case nutsdb.ErrPrefixSearchScan:
				WriteError(c, ErrPrefixSearchScan)
			default:
				WriteError(c, ErrUnknown)
			}
			return
		}
		var res = map[string]string{}
		for _, e := range entries {
			res[string(e.Key)] = string(e.Value)
		}
		WriteSucc(c, res)
	case RangeScan:
		type RangeScanReq struct {
			Start *string `json:"start" binding:"required"`
			End   *string `json:"end" binding:"required"`
		}
		var rangeScanReq RangeScanReq
		if err = c.ShouldBindJSON(&rangeScanReq); err != nil {
			WriteError(c, APIMessage{
				Message: err.Error(),
			})
			return
		}
		entries, err = s.core.RangeScan(scanParam.Bucket, *rangeScanReq.Start, *rangeScanReq.End)
		if err != nil {
			switch err {
			case nutsdb.ErrRangeScan:
				WriteError(c, ErrRangeScan)
			default:
				WriteError(c, ErrUnknown)
			}
			return
		}
		var res = map[string]string{}
		for _, e := range entries {
			res[string(e.Key)] = string(e.Value)
		}
		WriteSucc(c, res)
	case GetAll:
		entries, err = s.core.GetAll(scanParam.Bucket)
		if err != nil {
			switch err {
			case nutsdb.ErrBucketEmpty:
				WriteError(c, ErrBucketEmpty)
			default:
				WriteError(c, ErrUnknown)
			}
			return
		}
		var res = map[string]string{}
		for _, e := range entries {
			res[string(e.Key)] = string(e.Value)
		}
		WriteSucc(c, res)
	}

	return
}

func (s *NutsHTTPServer) DeleteOldFiles(c *gin.Context) {
	type DeleteRequest struct {
		Count int `uri:"count" binding:"required"`
	}
	var (
		err           error
		deleteRequest DeleteRequest
	)

	if err = c.ShouldBindUri(&deleteRequest); err != nil {
		WriteError(c, APIMessage{
			Message: err.Error(),
		})
		return
	}

	err = s.core.DeleteOldFiles(deleteRequest.Count)
	if err != nil {
		switch err {
		case nutsdb.ErrNotFoundKey:
			WriteError(c, ErrKeyNotFoundInBucket)
		default:
			WriteError(c, ErrUnknown)
		}
		return
	}
	WriteSucc(c, struct{}{})
}
