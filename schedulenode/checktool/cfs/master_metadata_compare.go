package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/compare_meta"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/compare_meta_dbbak"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/tar"
	"github.com/cubefs/cubefs/util/unit"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	lRUCacheSize    = 3 << 30
	writeBufferSize = 4 * unit.MB
)

const (
	cloudOssUrlFormat = "https://%v/chubaofs_master_backup/%v/%v/masterbackup_%v_%v.tgz"
	rocksDataPath     = "/tmp/rocksdata"
	rocksdbPrefix     = "rocksdb"
)

type CompareRocksMeta interface {
	RangePrefix(iter func(string))
	Compare(rocksPaths []string, prefix string, dbMap map[string]*raftstore.RocksDBStore, cluster string, umpKey string) string
}

func (s *ChubaoFSMonitor) checkMasterMetadata() {
	for domain, masterAddrs := range s.chubaoFSMasterNodes {
		if len(masterAddrs) < 2 {
			log.LogErrorf("parameter master address must be more than 2, actual:%v", len(masterAddrs))
			continue
		}
		cluster := getClusterName(domain)
		if cluster == "unknown" {
			log.LogErrorf("parameter cluster not found, domain:%v", domain)
			continue
		}
		os.RemoveAll(path.Join(rocksDataPath, cluster))
		os.MkdirAll(path.Join(rocksDataPath, cluster), 0777)
		executeCompare(masterAddrs, s.configMap[cfgKeyOssDomain], cluster, domain)
	}
}

func parseCloudOssUrl(host string, ossDomain, cluster string, lastBackupTime time.Time) string {
	dayTime := lastBackupTime.Format("20060102")
	tFlag := dayTime + "_" + fmt.Sprintf("%v-00", lastBackupTime.Hour())
	return fmt.Sprintf(cloudOssUrlFormat, ossDomain, cluster, dayTime, host, tFlag)
}

func downloadRocksdbBackup(host, url, cluster string) (err error) {
	log.LogInfof("downloading rocksdb data from " + url)
	// get header
	resp, err := http.Head(url)
	if err != nil {
		log.LogErrorf("resp, err := http.Head(strURL)  报错: strURL = %v", url)
		return
	}

	// get length
	fileLength := int(resp.ContentLength)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.LogErrorf(err.Error())
		return
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", 0, fileLength))

	// get data body to memory
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.LogErrorf("http.DefaultClient.Do(req), err:%v", err)
		return
	}
	defer resp.Body.Close()

	// open file
	gzFilename := filepath.Join(rocksDataPath, cluster, host+".tgz")
	flags := os.O_CREATE | os.O_RDWR
	var f *os.File
	f, err = os.OpenFile(gzFilename, flags, 0666)
	if err != nil {
		log.LogErrorf("create file failed")
		return
	}
	defer f.Close()

	// write data to file
	buf := make([]byte, 16*1024)
	_, err = io.CopyBuffer(f, resp.Body, buf)
	if err != nil {
		if err == io.EOF {
			log.LogErrorf("io.EOF")
			return
		}
		log.LogInfof(err.Error())
		return
	}
	return
}

func executeCompare(masterAddrs []string, ossDomain, cluster, domain string) {
	rocksFilePaths := make([]string, 0)
	lastBackupTime := time.Now().Add(-time.Minute * 10)
	for _, host := range masterAddrs {
		h := strings.Split(host, ":")[0]
		url := parseCloudOssUrl(h, ossDomain, cluster, lastBackupTime)
		err := downloadRocksdbBackup(h, url, cluster)
		if err != nil {
			log.LogErrorf("download err:" + err.Error())
			return
		}
		gzFilename := filepath.Join(rocksDataPath, cluster, h+".tgz")
		unZipPath := filepath.Join(rocksDataPath, cluster, h)
		err = tar.UnTar(unZipPath, gzFilename)
		if err != nil {
			log.LogErrorf("extractPackage err:" + err.Error())
			return
		}
		os.Remove(gzFilename)
		rocksDir := "export/App/chubaoio/master/rocksdbstore"
		from := filepath.Join(unZipPath, rocksDir)
		to := filepath.Join(rocksDataPath, cluster, rocksdbPrefix+h)
		log.LogInfof("rename from:" + from + " to:" + to)
		err = os.Rename(from, to)
		os.RemoveAll(unZipPath)
		rocksFilePaths = append(rocksFilePaths, to)
	}

	var err error
	dbMap := make(map[string]*raftstore.RocksDBStore, 0)
	for _, rPath := range rocksFilePaths {
		var rs *raftstore.RocksDBStore
		if rs, err = raftstore.NewRocksDBStore(rPath, lRUCacheSize, writeBufferSize); err != nil {
			return
		}
		dbMap[rPath] = rs
	}
	tmpdir := os.TempDir()
	fName := filepath.Join(tmpdir, fmt.Sprintf("diff_%v_%v", cluster, time.Now().Format("2006-01-02")))
	resultFD, _ := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, 0777)
	diff := 0
	defer func() {
		resultFD.Close()
		if diff == 0 {
			log.LogInfof("check passed with no differences")
			os.Remove(fName)
		}
	}()
	var compare CompareRocksMeta

	if isReleaseCluster(domain) {
		compare = compare_meta_dbbak.NewCompare(cluster, domain)
	} else {
		compare = compare_meta.NewCompare(cluster, domain)
	}
	compare.RangePrefix(func(s string) {
		resultStr := compare.Compare(rocksFilePaths, s, dbMap, cluster, UMPCFSMasterMetaCompareKey)
		if resultStr != "" {
			diff++
			resultFD.WriteString(resultStr)
		}
	})
	return
}
