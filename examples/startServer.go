package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"nutsDBMDHttp"

	nutsdb "github.com/finddiff/nutsDBMD"
	_ "net/http/pprof"
)

var addr = flag.String("d", ":8080", "http监听地址")
var mod = flag.String("m", "all", "kv索引储存模式:all kv都在内存,key k在内存,none kv都在文件中")
var sdtype = flag.String("htype", "critbit", "kv索引储存引擎:critbit Critbit数存储,bptree B+数存储, hashmap HashMap存储, skiplist 调表存储")
var loadMOd = flag.String("l", "map", "加模式:map MMap模式,file FileIO模式")
var loadPath = flag.String("p", "./nutsdb", "数据库路径")
var startIndex = flag.Int("sf", -1, "加载文件范围开始值,默认不限制")
var endIndex = flag.Int("ef", -1, "加载文件范围结束值,默认不限制")
var isBackup = flag.Bool("backup", false, "备份操作")
var backupDir = flag.String("bdir", "./back", "备份输出目录")
var order = flag.Int("order", 8, "设置BPtree的阶数")
var invalidDel = flag.Int("free", 30, "定时清楚失效Key, 单位秒,默认30秒")
var maxttl = flag.Uint("ttl", 172800, "最大ttl：0 为持久存储, 单位秒,默认172800秒 2天")
var h = flag.Bool("h", false, "this help")

func main() {
	flag.Parse()
	if *h {
		flag.Usage()
		return
	}

	opt := nutsdb.DefaultOptions
	opt.LoadFileStartNum = *startIndex
	opt.LoadFileEndNum = *endIndex
	opt.Order = *order
	//opt.SegmentSize = 8 * 1024 * 1024

	switch *mod {
	case "all":
		opt.EntryIdxMode = nutsdb.HintKeyValAndRAMIdxMode
	case "key":
		opt.EntryIdxMode = nutsdb.HintKeyAndRAMIdxMode
	case "none":
		opt.EntryIdxMode = nutsdb.HintBPTSparseIdxMode
	}

	switch *loadMOd {
	case "map":
		opt.StartFileLoadingMode = nutsdb.MMap
	case "file":
		opt.StartFileLoadingMode = nutsdb.FileIO
	}

	switch *sdtype {
	case "critbit":
		opt.HitMode = nutsdb.CritBit
	case "bptree":
		opt.HitMode = nutsdb.Bptree
	case "hashmap":
		opt.HitMode = nutsdb.HashMap
	case "skiplist":
		opt.HitMode = nutsdb.Skiplist
	}

	go func() {
		http.ListenAndServe("0.0.0.0:1234", nil)
	}()

	fmt.Printf("%s: server listen: %s mod:%v loadmod:%v\n", time.Now().Format("2006-01-02 15:04:05.000000"), *addr, opt.EntryIdxMode, opt.StartFileLoadingMode)
	opt.Dir = *loadPath
	if *isBackup {
		opt.BackUP = true
	}

	opt.InvalidDel = *invalidDel
	//opt.HitMode = nutsdb.CritBit
	opt.MaxTtl = uint32(*maxttl)
	db, err := nutsdb.Open(opt)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//if *isBackup {
	//	db.Merge()
	//	bkopt := nutsdb.DefaultOptions
	//	bkopt.EntryIdxMode = nutsdb.HintKeyAndRAMIdxMode
	//	bkopt.StartFileLoadingMode = nutsdb.FileIO
	//	bkopt.BackUP = true
	//	bkopt.Dir = *backupDir
	//	bkdb, bkerr := nutsdb.Open(bkopt)
	//
	//	if bkerr != nil {
	//		log.Fatal(bkerr)
	//		return
	//	}
	//	defer bkdb.Close()
	//
	//	if opt.EntryIdxMode == nutsdb.HintBPTSparseIdxMode {
	//		log.Fatal(nutsdb.ErrNotSupportHintBPTSparseIdxMode)
	//		return
	//	}
	//	if err := bkdb.MergeDB(db); err != nil {
	//		log.Fatal(err)
	//		return
	//	}
	//
	//	return
	//}

	//db.Merge()

	go func() {
		if err := nutshttp.Enable(db, *addr); err != nil {
			panic(err)
		}
	}()

	select {}
}
