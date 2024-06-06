// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package datanode

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/datanode/riskdata"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/diskusage"
	"github.com/cubefs/cubefs/util/ttlstore"
	"github.com/tiglabs/raft"
	raftProto "github.com/tiglabs/raft/proto"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"hash/crc32"
	"io"
	"io/fs"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/iputil"

	"github.com/cubefs/cubefs/util/topology"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/repl"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/async"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/cpu"
	utilErrors "github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/multirate"
	"github.com/cubefs/cubefs/util/statinfo"
	"github.com/cubefs/cubefs/util/statistics"
	"github.com/cubefs/cubefs/util/unit"
)

var (
	ErrIncorrectStoreType       = errors.New("Incorrect store type")
	ErrNoSpaceToCreatePartition = errors.New("No disk space to create a data partition")
	ErrNewSpaceManagerFailed    = errors.New("Creater new space manager failed")
	ErrPartitionNil             = errors.New("partition is nil")
	LocalIP                     string
	LocalServerPort             string
	gConnPool                   = connpool.NewConnectPool()
	MasterClient                = masterSDK.NewMasterClient(nil, false)
	MasterLBDomainClient        = masterSDK.NewMasterClientWithLBDomain("", false)
	gHasLoadDataPartition       bool
	gHasFinishedLoadDisks       bool

	maybeServerFaultOccurred bool // 是否判定当前节点大概率出现过系统断电
)

var (
	AutoRepairStatus = true
)

const (
	DefaultZoneName          = proto.DefaultZoneName
	DefaultRaftLogsToRetain  = 1000 // Count of raft logs per data partition
	DefaultDiskMaxErr        = 1
	DefaultDiskReservedSpace = 5 * unit.GB // GB
	DefaultDiskUsableRatio   = float64(0.90)
	DefaultDiskReservedRatio = 0.1
)

const (
	ModuleName          = "dataNode"
	SystemStartTimeFile = "SYS_START_TIME"
	MAX_OFFSET_OF_TIME  = 5
)

const (
	ConfigKeyLocalIP        = "localIP"        // string
	ConfigKeyPort           = "port"           // int
	ConfigKeyMasterAddr     = "masterAddr"     // array
	ConfigKeyZone           = "zoneName"       // string
	ConfigKeyDisks          = "disks"          // array
	ConfigKeyRaftDir        = "raftDir"        // string
	ConfigKeyRaftHeartbeat  = "raftHeartbeat"  // string
	ConfigKeyRaftReplica    = "raftReplica"    // string
	cfgTickIntervalMs       = "tickIntervalMs" // int
	ConfigKeyMasterLBDomain = "masterLBDomain"
	ConfigKeyEnableRootDisk = "enableRootDisk"
)

// DataNode defines the structure of a data node.
type DataNode struct {
	space                    *SpaceManager
	port                     string
	httpPort                 string
	zoneName                 string
	clusterID                string
	localIP                  string
	localServerAddr          string
	nodeID                   uint64
	raftDir                  string
	raftHeartbeat            string
	raftReplica              string
	raftStore                raftstore.RaftStore
	tickInterval             int
	tcpListener              net.Listener
	stopC                    chan bool
	fixTinyDeleteRecordLimit uint64
	control                  common.Control
	processStatInfo          *statinfo.ProcessStatInfo
	topoManager              *topology.TopologyManager
	transferDeleteLock       sync.Mutex
}

func NewServer() *DataNode {
	return &DataNode{}
}

func (s *DataNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return s.control.Start(s, cfg, doStart)
}

// Shutdown shuts down the current data node.
func (s *DataNode) Shutdown() {
	s.control.Shutdown(s, doShutdown)
}

// Sync keeps data node in sync.
func (s *DataNode) Sync() {
	s.control.Sync()
}

// Workflow of starting up a data node.
func doStart(server common.Server, cfg *config.Config) (err error) {
	s, ok := server.(*DataNode)
	if !ok {
		return errors.New("Invalid Node Type!")
	}
	s.stopC = make(chan bool, 0)
	if err = s.parseSysStartTime(); err != nil {
		return
	}
	// parse the config file
	if err = s.parseConfig(cfg); err != nil {
		return
	}
	repl.SetConnectPool(gConnPool)
	if err = s.register(); err != nil {
		err = fmt.Errorf("regiter failed: %v", err)
		return
	}
	exporter.Init(exporter.NewOptionFromConfig(cfg).WithCluster(s.clusterID).WithModule(ModuleName).WithZone(s.zoneName))

	_, err = multirate.InitLimiterManager(multirate.ModuleDataNode, s.zoneName, MasterClient.AdminAPI().GetLimitInfo)
	if err != nil {
		return err
	}
	s.topoManager = topology.NewTopologyManager(0, 0, MasterClient, MasterLBDomainClient,
		false, false)
	if err = s.topoManager.Start(); err != nil {
		return
	}

	// start the raft server
	if err = s.startRaftServer(cfg); err != nil {
		return
	}

	// create space manager (disk, partition, etc.)
	if err = s.startSpaceManager(cfg); err != nil {
		exporter.Warning(err.Error())
		return
	}

	// start tcp listening
	if err = s.startTCPService(); err != nil {
		return
	}

	log.LogErrorf("doStart startTCPService finish")

	// Start all loaded data partitions which managed by space manager,
	// this operation will start raft partitions belong to data partitions.
	s.space.StartPartitions()

	async.RunWorker(s.space.AsyncLoadExtent)
	async.RunWorker(s.registerHandler)
	async.RunWorker(s.startUpdateNodeInfo)
	async.RunWorker(s.startUpdateProcessStatInfo)

	statistics.InitStatistics(cfg, s.clusterID, statistics.ModelDataNode, s.zoneName, LocalIP, s.rangeMonitorData)

	return
}

func doShutdown(server common.Server) {
	s, ok := server.(*DataNode)
	if !ok {
		return
	}
	close(s.stopC)
	s.space.Stop()
	s.stopUpdateNodeInfo()
	s.stopTCPService()
	s.stopRaftServer()
	if gHasFinishedLoadDisks {
		deleteSysStartTimeFile()
	}
	multirate.Stop()
}

func (s *DataNode) parseConfig(cfg *config.Config) (err error) {
	var (
		port       string
		regexpPort *regexp.Regexp
	)
	LocalIP = cfg.GetString(ConfigKeyLocalIP)
	port = cfg.GetString(proto.ListenPort)
	LocalServerPort = port
	if regexpPort, err = regexp.Compile("^(\\d)+$"); err != nil {
		return fmt.Errorf("Err:no port")
	}
	if !regexpPort.MatchString(port) {
		return fmt.Errorf("Err:port must string")
	}
	s.port = port
	s.httpPort = cfg.GetString(proto.HttpPort)

	masterDomain, masterAddrs := s.parseMasterAddrs(cfg)
	if len(masterAddrs) == 0 {
		return fmt.Errorf("Err:masterAddr unavalid")
	}

	for _, ip := range masterAddrs {
		MasterClient.AddNode(ip)
	}
	MasterClient.SetMasterDomain(masterDomain)

	masterLBDomain := s.parseMasterLBDomain(cfg)
	if masterLBDomain != "" {
		MasterLBDomainClient.AddNode(masterLBDomain)
	}
	s.zoneName = cfg.GetString(ConfigKeyZone)
	if s.zoneName == "" {
		s.zoneName = DefaultZoneName
	}

	s.tickInterval = int(cfg.GetFloat(cfgTickIntervalMs))
	if s.tickInterval <= 300 {
		log.LogWarnf("DataNode: get config %s(%v) less than 300 so set it to 500 ", cfgTickIntervalMs, cfg.GetString(cfgTickIntervalMs))
		s.tickInterval = 500
	}

	log.LogInfof("DataNode: parse config: masterAddrs %v ", MasterClient.Nodes())
	log.LogInfof("DataNode: parse config: master domain %s", masterDomain)
	log.LogInfof("DataNode: parse config: master lb domain %s", masterLBDomain)
	log.LogInfof("DataNode: parse config: port %v", s.port)
	log.LogInfof("DataNode: parse config: zoneName %v ", s.zoneName)
	return
}

func (s *DataNode) parseMasterAddrs(cfg *config.Config) (masterDomain string, masterAddrs []string) {
	var err error
	masterDomain = cfg.GetString(proto.MasterDomain)
	if masterDomain != "" && !strings.Contains(masterDomain, ":") {
		masterDomain = masterDomain + ":" + proto.MasterDefaultPort
	}

	masterAddrs, err = iputil.LookupHost(masterDomain)
	if err != nil {
		masterAddrs = cfg.GetStringSlice(proto.MasterAddr)
	}
	return
}

func (s *DataNode) parseMasterLBDomain(cfg *config.Config) (masterLBDomain string) {
	return cfg.GetString(proto.MasterLBDomain)
}

// parseSysStartTime maybeServerFaultOccurred is set true only in these two occasions:
// system power off, then restart
// kill -9 the program, then reboot or power off, then restart
func (s *DataNode) parseSysStartTime() (err error) {
	baseDir := getBasePath()
	sysStartFile := path.Join(baseDir, SystemStartTimeFile)
	if _, err = os.Stat(sysStartFile); err != nil {
		if !os.IsNotExist(err) {
			return
		}
		maybeServerFaultOccurred = false
		if err = initSysStartTimeFile(); err != nil {
			log.LogErrorf("parseSysStartTime set system start time has err:%v", err)
		}
	} else {
		bs, err := ioutil.ReadFile(sysStartFile)
		if err != nil {
			return err
		}
		if len(bs) == 0 {
			maybeServerFaultOccurred = false
			if err = initSysStartTimeFile(); err != nil {
				log.LogErrorf("parseSysStartTime set system start time has err:%v", err)
			}
			return err
		}
		localSysStart, err := strconv.ParseInt(strings.TrimSpace(string(bs)), 10, 64)
		if err != nil {
			return err
		}
		newSysStart, err := cpu.SysStartTime()
		if err != nil {
			return err
		}
		log.LogInfof("DataNode: load sys start time: record %d, current %d", localSysStart, newSysStart)

		if maybeServerFaultOccurred = newSysStart-localSysStart > MAX_OFFSET_OF_TIME; maybeServerFaultOccurred {
			log.LogWarnf("DataNode: the program may be started after power off, record %d, current %d", localSysStart, newSysStart)
		}
	}
	return
}

func (s *DataNode) startSpaceManager(cfg *config.Config) (err error) {
	s.space = NewSpaceManager(s)
	if len(strings.TrimSpace(s.port)) == 0 {
		err = ErrNewSpaceManagerFailed
		return
	}

	var limitInfo *proto.LimitInfo
	limitInfo, err = MasterClient.AdminAPI().GetLimitInfo("")
	if err == nil && limitInfo != nil {
		s.space.SetDiskReservedRatio(limitInfo.DataNodeDiskReservedRatio)
	}

	s.space.SetRaftStore(s.raftStore)
	s.space.SetNodeID(s.nodeID)
	s.space.SetClusterID(s.clusterID)

	var startTime = time.Now()

	// Prepare and validate disk config
	var getDeviceID = func(path string) (devID uint64, err error) {
		var stat = new(syscall.Stat_t)
		if err = syscall.Stat(path, stat); err != nil {
			return
		}
		devID = stat.Dev
		return
	}

	var rootDevID uint64
	if rootDevID, err = getDeviceID("/"); err != nil {
		return
	}
	log.LogInfof("root device: / (%v)", rootDevID)

	var diskPaths = make(map[uint64]*DiskPath) // DevID -> DiskPath
	for _, d := range cfg.GetSlice(ConfigKeyDisks) {
		var diskPath, ok = ParseDiskPath(d.(string))
		if !ok {
			err = fmt.Errorf("invalid disks configuration: %v", d)
			return
		}
		var devID, derr = getDeviceID(diskPath.Path())
		if derr != nil {
			log.LogErrorf("get device ID for %v failed: %v", diskPath.Path(), derr)
			continue
		}
		if !cfg.GetBool(ConfigKeyEnableRootDisk) && devID == rootDevID {
			err = fmt.Errorf("root device in disks configuration: %v (%v), ", d, devID)
			return
		}
		if p, exists := diskPaths[devID]; exists {
			err = fmt.Errorf("dependent device in disks configuration: [%v,%v]", d, p.Path())
			return
		}

		log.LogInfof("disk device: %v, path %v, device %v", d, diskPath.Path(), devID)
		diskPaths[devID] = diskPath
	}

	var checkExpired CheckExpired
	var requires, fetchErr = s.fetchPersistPartitionIDsFromMaster()
	if fetchErr == nil {
		checkExpired = func(id uint64) bool {
			if len(requires) == 0 {
				return true
			}
			for _, existId := range requires {
				if existId == id {
					return false
				}
			}
			return true
		}
	}

	var futures = make(map[string]*async.Future)
	for devID, diskPath := range diskPaths {
		var future = async.NewFuture()
		go func(path string, future *async.Future) {
			if log.IsInfoEnabled() {
				log.LogInfof("SPCMGR: loading disk: devID=%v, path=%v", devID, diskPath)
			}
			var err = s.space.LoadDisk(path, checkExpired)
			future.Respond(nil, err)
		}(diskPath.Path(), future)
		futures[diskPath.Path()] = future
	}
	for p, future := range futures {
		if _, ferr := future.Response(); ferr != nil {
			log.LogErrorf("load disk [%v] failed: %v", p, ferr)
			continue
		}
	}

	// Check missed partitions
	var misses = make([]uint64, 0)
	for _, id := range requires {
		if dp := s.space.Partition(id); dp == nil {
			misses = append(misses, id)
		}
	}
	if len(misses) > 0 {
		err = fmt.Errorf("lack partitions: %v", misses)
		return
	}

	deleteSysStartTimeFile()
	_ = initSysStartTimeFile()
	maybeServerFaultOccurred = false

	gHasFinishedLoadDisks = true
	log.LogInfof("SPCMGR: loaded all %v disks elapsed %v", len(diskPaths), time.Since(startTime))
	return nil
}

func (s *DataNode) fetchPersistPartitionIDsFromMaster() (ids []uint64, err error) {
	var dataNode *proto.DataNodeInfo
	for i := 0; i < 3; i++ {
		dataNode, err = MasterClient.NodeAPI().GetDataNode(s.localServerAddr)
		if err != nil {
			log.LogErrorf("DataNode: fetch node info from master failed: %v", err)
			continue
		}
		break
	}
	if err != nil {
		return
	}
	ids = dataNode.PersistenceDataPartitions
	return
}

// registers the data node on the master to report the information such as IsIPV4 address.
// The startup of a data node will be blocked until the registration succeeds.
func (s *DataNode) register() (err error) {
	var (
		regInfo = &masterSDK.RegNodeInfoReq{
			Role:     proto.RoleData,
			ZoneName: s.zoneName,
			Version:  DataNodeLatestVersion,
			ProfPort: s.httpPort,
			SrvPort:  s.port,
		}
		regRsp *proto.RegNodeRsp
	)

	for retryCount := registerMaxRetryCount; retryCount > 0; retryCount-- {
		regRsp, err = MasterClient.RegNodeInfo(proto.AuthFilePath, regInfo)
		if err == nil {
			break
		}
		time.Sleep(registerRetryWaitInterval)
	}

	if err != nil {
		log.LogErrorf("DataNode register failed: %v", err)
		return
	}
	ipAddr := strings.Split(regRsp.Addr, ":")[0]
	if !unit.IsIPV4(ipAddr) {
		err = fmt.Errorf("got invalid local IP %v fetched from Master", ipAddr)
		return
	}
	s.clusterID = regRsp.Cluster
	if LocalIP == "" {
		LocalIP = ipAddr
	}
	s.localServerAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)
	s.nodeID = regRsp.Id
	if err = iputil.VerifyLocalIP(LocalIP); err != nil {
		log.LogErrorf("DataNode register verify local ip failed: %v", err)
		return
	}
	return
}

func (s *DataNode) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("DataNode")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc("/disks", s.getDiskAPI)
	http.HandleFunc("/partitions", s.getPartitionsAPI)
	http.HandleFunc("/partition", s.getPartitionAPI)
	http.HandleFunc("/partitionSimple", s.getPartitionSimpleAPI)
	http.HandleFunc("/triggerPartitionError", s.triggerPartitionError)
	http.HandleFunc("/partitionRaftHardState", s.getPartitionRaftHardStateAPI)
	http.HandleFunc("/extent", s.getExtentAPI)
	http.HandleFunc("/block", s.getBlockCrcAPI)
	http.HandleFunc("/stats", s.getStatAPI)
	http.HandleFunc("/raftStatus", s.getRaftStatus)
	http.HandleFunc("/setAutoRepairStatus", s.setAutoRepairStatus)
	http.HandleFunc("/releasePartitions", s.releasePartitions)
	http.HandleFunc("/restorePartitions", s.restorePartitions)
	http.HandleFunc("/computeExtentMd5", s.getExtentMd5Sum)
	http.HandleFunc("/stat/info", s.getStatInfo)
	http.HandleFunc("/getReplBufferDetail", s.getReplProtocalBufferDetail)
	http.HandleFunc("/tinyExtentHoleInfo", s.getTinyExtentHoleInfo)
	http.HandleFunc("/playbackTinyExtentMarkDelete", s.playbackPartitionTinyDelete)
	http.HandleFunc("/stopPartition", s.stopPartition)
	http.HandleFunc("/reloadPartition", s.reloadPartition)
	http.HandleFunc("/moveExtent", s.moveExtentFile)
	http.HandleFunc("/moveExtentBatch", s.moveExtentFileBatch)
	http.HandleFunc("/repairExtent", s.repairExtent)
	http.HandleFunc("/repairExtentBatch", s.repairExtentBatch)
	http.HandleFunc("/extentCrc", s.getExtentCrc)
	http.HandleFunc("/fingerprint", s.getFingerprint)
	http.HandleFunc("/resetFaultOccurredCheckLevel", s.resetFaultOccurredCheckLevel)
	http.HandleFunc("/sfxStatus", s.getSfxStatus)
	http.HandleFunc("/getExtentLockInfo", s.getExtentLockInfo)
	http.HandleFunc("/transferDeleteV0", s.transferDeleteV0)
	http.HandleFunc("/risk/status", s.getRiskStatus)
	http.HandleFunc("/risk/startFix", s.startRiskFix)
	http.HandleFunc("/risk/stopFix", s.stopRiskFix)
	http.HandleFunc("/getDataPartitionViewCache", s.getDataPartitionViewCache)
}

func (s *DataNode) startTCPService() (err error) {
	log.LogInfo("Start: startTCPService")
	addr := fmt.Sprintf(":%v", s.port)
	l, err := net.Listen(NetworkProtocol, addr)
	log.LogDebugf("action[startTCPService] listen %v address(%v).", NetworkProtocol, addr)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	s.tcpListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[startTCPService] failed to accept, err:%s", err.Error())
				time.Sleep(time.Second * 5)
				continue
			}
			log.LogDebugf("action[startTCPService] accept connection from %s.", conn.RemoteAddr().String())
			go s.serveConn(conn)
		}
	}(l)
	return
}

func (s *DataNode) stopTCPService() (err error) {
	if s.tcpListener != nil {

		s.tcpListener.Close()
		log.LogDebugf("action[stopTCPService] stop tcp service.")
	}
	return
}

func (s *DataNode) serveConn(conn net.Conn) {
	space := s.space
	space.Stats().AddConnection()
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	packetProcessor := repl.NewReplProtocol(c, s.Prepare, s.OperatePacket, s.Post)
	packetProcessor.ServerConn()
}

// Increase the disk error count by one.
func (s *DataNode) incDiskErrCnt(partitionID uint64, err error, flag uint8) {
	if err == nil {
		return
	}
	dp := s.space.Partition(partitionID)
	if dp == nil {
		return
	}
	d := dp.Disk()
	if d == nil {
		return
	}
	if !IsDiskErr(err) {
		return
	}
	if flag == WriteFlag {
		d.incWriteErrCnt()
	} else if flag == ReadFlag {
		d.incReadErrCnt()
	}
}

var (
	staticReflectedErrnoType = reflect.TypeOf(syscall.Errno(0))
)

func IsSysErr(err error) (is bool) {
	if err == nil {
		return
	}
	return reflect.TypeOf(err) == staticReflectedErrnoType
}

func IsDiskErr(err error) bool {
	return err != nil &&
		(strings.Contains(err.Error(), syscall.EIO.Error()) ||
			strings.Contains(err.Error(), syscall.EROFS.Error()))
}

func (s *DataNode) rangeMonitorData(deal func(data *statistics.MonitorData, vol, path string, pid uint64)) {
	s.space.WalkDisks(func(disk *Disk) bool {
		for _, md := range disk.monitorData {
			deal(md, "", disk.Path, 0)
		}
		return true
	})

	s.space.WalkPartitions(func(partition *DataPartition) bool {
		for _, md := range partition.monitorData {
			deal(md, partition.volumeID, partition.Disk().Path, partition.partitionID)
		}
		return true
	})
}

func getBasePath() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	return dir
}

func initSysStartTimeFile() (err error) {
	baseDir := getBasePath()
	sysStartTime, err := cpu.SysStartTime()
	if err != nil {
		return
	}
	if err = ioutil.WriteFile(path.Join(baseDir, SystemStartTimeFile), []byte(strconv.FormatUint(uint64(sysStartTime), 10)), os.ModePerm); err != nil {
		return err
	}
	return
}

func deleteSysStartTimeFile() {
	baseDir := getBasePath()
	os.Remove(path.Join(baseDir, SystemStartTimeFile))
}

const (
	DefaultMarkDeleteLimitBurst           = 512
	UpdateNodeBaseInfoTicket              = 1 * time.Minute
	DefaultFixTinyDeleteRecordLimitOnDisk = 1
	DefaultNormalExtentDeleteExpireTime   = 4 * 3600
	DefaultLazyLoadParallelismPerDisk     = 2
)

var (
	nodeInfoStopC     = make(chan struct{}, 0)
	deleteLimiteRater = rate.NewLimiter(rate.Inf, DefaultMarkDeleteLimitBurst)
	logMaxSize        uint64
)

func (s *DataNode) startUpdateNodeInfo() {
	updateNodeBaseInfoTicker := time.NewTicker(UpdateNodeBaseInfoTicket)
	defer func() {
		updateNodeBaseInfoTicker.Stop()
	}()

	// call once on init before first tick
	s.updateNodeBaseInfo()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("datanode nodeinfo goroutine stopped")
			return
		case <-updateNodeBaseInfoTicker.C:
			s.updateNodeBaseInfo()
		}
	}
}

func (s *DataNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (s *DataNode) updateNodeBaseInfo() {
	//todo: better using a light weighted interface
	limitInfo, err := MasterClient.AdminAPI().GetLimitInfo("")
	if err != nil {
		log.LogWarnf("[updateNodeBaseInfo] get limit info err: %s", err.Error())
		return
	}

	r := limitInfo.DataNodeDeleteLimitRate
	l := rate.Limit(r)
	if r == 0 {
		l = rate.Inf
	}
	deleteLimiteRater.SetLimit(l)

	s.space.SetDiskFixTinyDeleteRecordLimit(limitInfo.DataNodeFixTinyDeleteRecordLimitOnDisk)
	s.space.SetForceFlushFDInterval(limitInfo.DataNodeFlushFDInterval)
	s.space.SetSyncWALOnUnstableEnableState(limitInfo.DataSyncWALOnUnstableEnableState)
	s.space.SetForceFlushFDParallelismOnDisk(limitInfo.DataNodeFlushFDParallelismOnDisk)
	s.space.SetPartitionConsistencyMode(limitInfo.DataPartitionConsistencyMode)

	s.space.SetNormalExtentDeleteExpireTime(limitInfo.DataNodeNormalExtentDeleteExpire)
	if statistics.StatisticsModule != nil {
		statistics.StatisticsModule.UpdateMonitorSummaryTime(limitInfo.MonitorSummarySec)
		statistics.StatisticsModule.UpdateMonitorReportTime(limitInfo.MonitorReportSec)
	}

	s.updateLogMaxSize(limitInfo.LogMaxSize)

	if s.topoManager != nil {
		s.topoManager.UpdateFetchTimerIntervalMin(limitInfo.TopologyFetchIntervalMin, limitInfo.TopologyForceFetchIntervalSec)
	}

	s.space.SetDiskReservedRatio(limitInfo.DataNodeDiskReservedRatio)
}

func (s *DataNode) updateLogMaxSize(val uint64) {
	if val != 0 && logMaxSize != val {
		oldLogMaxSize := logMaxSize
		logMaxSize = val
		log.SetLogMaxSize(int64(val))
		log.LogInfof("updateLogMaxSize, logMaxSize(old:%v, new:%v)", oldLogMaxSize, logMaxSize)
	}
}

func DeleteLimiterWait() {
	deleteLimiteRater.Wait(context.Background())
}

func (s *DataNode) startUpdateProcessStatInfo() {
	s.processStatInfo = statinfo.NewProcessStatInfo()
	s.processStatInfo.ProcessStartTime = time.Now().Format("2006-01-02 15:04:05")
	go s.processStatInfo.UpdateStatInfoSchedule()
}

func (s *DataNode) parseRaftConfig(cfg *config.Config) (err error) {
	s.raftDir = cfg.GetString(ConfigKeyRaftDir)
	if s.raftDir == "" {
		return fmt.Errorf("bad raftDir config")
	}
	s.raftHeartbeat = cfg.GetString(ConfigKeyRaftHeartbeat)
	s.raftReplica = cfg.GetString(ConfigKeyRaftReplica)
	log.LogDebugf("[parseRaftConfig] load raftDir(%v).", s.raftDir)
	log.LogDebugf("[parseRaftConfig] load raftHearbeat(%v).", s.raftHeartbeat)
	log.LogDebugf("[parseRaftConfig] load raftReplica(%v).", s.raftReplica)
	return
}

func (s *DataNode) startRaftServer(cfg *config.Config) (err error) {
	log.LogInfo("Start: startRaftServer")

	s.parseRaftConfig(cfg)

	constCfg := config.ConstConfig{
		Listen:           s.port,
		RaftHeartbetPort: s.raftHeartbeat,
		RaftReplicaPort:  s.raftReplica,
	}
	var ok = false
	if ok, err = config.CheckOrStoreConstCfg(s.raftDir, config.DefaultConstConfigFile, &constCfg); !ok {
		log.LogErrorf("constCfg check failed %v %v %v %v", s.raftDir, config.DefaultConstConfigFile, constCfg, err)
		return fmt.Errorf("constCfg check failed %v %v %v %v", s.raftDir, config.DefaultConstConfigFile, constCfg, err)
	}

	if _, err = os.Stat(s.raftDir); err != nil {
		if err = os.MkdirAll(s.raftDir, 0755); err != nil {
			err = utilErrors.NewErrorf("create raft server dir: %s", err.Error())
			log.LogErrorf("action[startRaftServer] cannot start raft server err(%v)", err)
			return
		}
	}

	heartbeatPort, err := strconv.Atoi(s.raftHeartbeat)
	if err != nil {
		err = utilErrors.NewErrorf("Raft heartbeat port configuration error: %s", err.Error())
		return
	}
	replicatePort, err := strconv.Atoi(s.raftReplica)
	if err != nil {
		err = utilErrors.NewErrorf("Raft replica port configuration error: %s", err.Error())
		return
	}

	raftConf := &raftstore.Config{
		NodeID:            s.nodeID,
		RaftPath:          s.raftDir,
		TickInterval:      s.tickInterval,
		IPAddr:            LocalIP,
		HeartbeatPort:     heartbeatPort,
		ReplicaPort:       replicatePort,
		NumOfLogsToRetain: DefaultRaftLogsToRetain,
	}
	s.raftStore, err = raftstore.NewRaftStore(raftConf)
	if err != nil {
		err = utilErrors.NewErrorf("new raftStore: %s", err.Error())
		log.LogErrorf("action[startRaftServer] cannot start raft server err(%v)", err)
	}

	return
}

func (s *DataNode) stopRaftServer() {
	if s.raftStore != nil {
		s.raftStore.Stop()
	}
}

// wrap_operator
func (s *DataNode) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size

	tpObject := exporter.NewModuleTP(p.GetOpMsg())
	tpObj := BeforeTpMonitor(p)

	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op(%v) error(%v)", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			switch p.Opcode {
			case proto.OpStreamRead, proto.OpRead, proto.OpExtentRepairRead, proto.OpStreamFollowerRead:
			case proto.OpReadTinyDeleteRecord:
				if log.IsReadEnabled() {
					log.LogReadf("action[OperatePacket] %v.",
						p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
				}
			case proto.OpWrite, proto.OpRandomWrite, proto.OpSyncRandomWrite, proto.OpSyncWrite, proto.OpMarkDelete:
				if log.IsWriteEnabled() {
					log.LogWritef("action[OperatePacket] %v.",
						p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
				}
			default:
				if log.IsInfoEnabled() {
					log.LogInfof("action[OperatePacket] %v.",
						p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
				}
			}
		}
		if p.Opcode == proto.OpMarkDelete {
			tpObj.AfterTp(0)
		} else {
			tpObj.AfterTp(uint64(sz))
		}
		p.Size = resultSize
		tpObject.Set(err)
	}()

	switch p.Opcode {
	case proto.OpCreateExtent:
		s.handlePacketToCreateExtent(p)
	case proto.OpWrite, proto.OpSyncWrite:
		s.handleWritePacket(p)
	case proto.OpStreamRead:
		s.handleStreamReadPacket(p, c, StreamRead)
	case proto.OpStreamFollowerRead:
		s.handleStreamFollowerReadPacket(p, c, StreamRead)
	case proto.OpExtentRepairRead:
		s.handleExtentRepairReadPacket(p, c, RepairRead)
	case proto.OpTinyExtentRepairRead:
		s.handleTinyExtentRepairRead(p, c)
	case proto.OpTinyExtentAvaliRead:
		s.handleTinyExtentAvaliRead(p, c)
	case proto.OpMarkDelete:
		s.handleMarkDeletePacket(p, c)
	case proto.OpBatchDeleteExtent:
		s.handleBatchMarkDeletePacket(p, c)
	case proto.OpRandomWrite, proto.OpSyncRandomWrite:
		s.handleRandomWritePacket(p)
	case proto.OpLockOrUnlockExtent:
		s.handleLockOrUnlockExtent(p, c)
	case proto.OpNotifyReplicasToRepair:
		s.handlePacketToNotifyExtentRepair(p)
	case proto.OpGetAllWatermarks:
		s.handlePacketToGetAllWatermarks(p)
	case proto.OpGetAllWatermarksV2:
		s.handlePacketToGetAllWatermarksV2(p)
	case proto.OpGetAllWatermarksV3:
		s.handlePacketToGetAllWatermarksV3(p)
	case proto.OpFingerprint:
		s.handleFingerprintPacket(p)
	case proto.OpGetAllExtentInfo:
		s.handlePacketToGetAllExtentInfo(p)
	case proto.OpCreateDataPartition:
		s.handlePacketToCreateDataPartition(p)
	case proto.OpLoadDataPartition:
		s.handlePacketToLoadDataPartition(p)
	case proto.OpDeleteDataPartition:
		s.handlePacketToDeleteDataPartition(p)
	case proto.OpDataNodeHeartbeat:
		s.handleHeartbeatPacket(p)
	case proto.OpGetAppliedId:
		s.handlePacketToGetAppliedID(p)
	case proto.OpGetPersistedAppliedId:
		s.handlePacketToGetPersistedAppliedID(p)
	case proto.OpDecommissionDataPartition:
		s.handlePacketToDecommissionDataPartition(p)
	case proto.OpAddDataPartitionRaftMember:
		s.handlePacketToAddDataPartitionRaftMember(p)
	case proto.OpRemoveDataPartitionRaftMember:
		s.handlePacketToRemoveDataPartitionRaftMember(p)
	case proto.OpAddDataPartitionRaftLearner:
		s.handlePacketToAddDataPartitionRaftLearner(p)
	case proto.OpPromoteDataPartitionRaftLearner:
		s.handlePacketToPromoteDataPartitionRaftLearner(p)
	case proto.OpResetDataPartitionRaftMember:
		s.handlePacketToResetDataPartitionRaftMember(p)
	case proto.OpDataPartitionTryToLeader:
		s.handlePacketToDataPartitionTryToLeader(p)
	case proto.OpGetPartitionSize:
		s.handlePacketToGetPartitionSize(p)
	case proto.OpGetMaxExtentIDAndPartitionSize:
		s.handlePacketToGetMaxExtentIDAndPartitionSize(p)
	case proto.OpReadTinyDeleteRecord:
		s.handlePacketToReadTinyDeleteRecordFile(p, c)
	case proto.OpBroadcastMinAppliedID:
		s.handleBroadcastMinAppliedID(p)
	case proto.OpSyncDataPartitionReplicas:
		s.handlePacketToSyncDataPartitionReplicas(p)
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}

	return
}

// Handle OpCreateExtent packet.
func (s *DataNode) handlePacketToCreateExtent(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateExtent, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*DataPartition)
	if err = partition.CheckWritable(); err != nil {
		return
	}

	var inode uint64
	if p.Size >= 8 {
		inode = binary.BigEndian.Uint64(p.Data[0:8])
	}

	err = partition.ExtentStore().Create(p.ExtentID, inode, true)
	return
}

// Handle OpCreateDataPartition packet.
func (s *DataNode) handlePacketToCreateDataPartition(p *repl.Packet) {
	var (
		err   error
		bytes []byte
		dp    *DataPartition
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateDataPartition, err.Error())
		}
	}()
	task := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, task); err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	request := &proto.CreateDataPartitionRequest{}
	if task.OpCode != proto.OpCreateDataPartition {
		err = fmt.Errorf("from master Task(%v) failed,error unavali opcode(%v)", task.ToString(), task.OpCode)
		return
	}

	bytes, err = json.Marshal(task.Request)
	if err != nil {
		err = fmt.Errorf("from master Task(%v) cannot unmashal CreateDataPartition", task.ToString())
		return
	}
	p.AddMesgLog(string(bytes))
	if err = json.Unmarshal(bytes, request); err != nil {
		err = fmt.Errorf("from master Task(%v) cannot unmash CreateDataPartitionRequest struct", task.ToString())
		return
	}
	p.PartitionID = request.PartitionId
	if dp, err = s.space.CreatePartition(request); err != nil {
		err = fmt.Errorf("from master Task(%v) cannot create Partition err(%v)", task.ToString(), err)
		return
	}
	p.PacketOkWithBody([]byte(dp.Disk().Path))

	return
}

// Handle OpHeartbeat packet.
func (s *DataNode) handleHeartbeatPacket(p *repl.Packet) {
	var err error
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateDataPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}

	go func() {
		if task.IsHeartBeatPbRequest {
			s.responseHeartbeatPb(task)
		} else {
			s.responseHeartbeat(task)
		}
	}()

}

// Handle OpDeleteDataPartition packet.
func (s *DataNode) handlePacketToDeleteDataPartition(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDeleteDataPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}
	request := &proto.DeleteDataPartitionRequest{}
	if task.OpCode == proto.OpDeleteDataPartition {
		bytes, _ := json.Marshal(task.Request)
		p.AddMesgLog(string(bytes))
		err = json.Unmarshal(bytes, request)
		if err != nil {
			return
		} else {
			s.space.ExpiredPartition(request.PartitionId)
		}
	} else {
		err = fmt.Errorf("illegal opcode ")
	}
	if err != nil {
		err = utilErrors.Trace(err, "delete DataPartition failed,PartitionID(%v)", request.PartitionId)
		log.LogErrorf("action[handlePacketToDeleteDataPartition] err(%v).", err)
	}
	log.LogInfof(fmt.Sprintf("action[handlePacketToDeleteDataPartition] %v error(%v)", request.PartitionId, err))

}

// Handle OpLoadDataPartition packet.
func (s *DataNode) handlePacketToLoadDataPartition(p *repl.Packet) {
	task := &proto.AdminTask{}
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionLoadDataPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	err = json.Unmarshal(p.Data, task)
	p.PacketOkReply()
	go s.asyncLoadDataPartition(task)
}

func (s *DataNode) asyncLoadDataPartition(task *proto.AdminTask) {
	var (
		err error
	)
	request := &proto.LoadDataPartitionRequest{}
	response := &proto.LoadDataPartitionResponse{}
	if task.OpCode == proto.OpLoadDataPartition {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		dp := s.space.Partition(request.PartitionId)
		if dp == nil {
			response.Status = proto.TaskFailed
			response.PartitionId = uint64(request.PartitionId)
			err = fmt.Errorf(fmt.Sprintf("DataPartition(%v) not found", request.PartitionId))
			response.Result = err.Error()
		} else {
			response = dp.Load()
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskSucceeds
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFailed
		err = fmt.Errorf("illegal opcode")
		response.Result = err.Error()
	}
	task.Response = response
	if err = MasterClient.NodeAPI().ResponseDataNodeTask(task); err != nil {
		err = utilErrors.Trace(err, "load DataPartition failed,PartitionID(%v)", request.PartitionId)
		log.LogError(utilErrors.Stack(err))
	}
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleMarkDeletePacket(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	remote := c.RemoteAddr().String()
	partition := p.Object.(*DataPartition)
	if p.ExtentType == proto.TinyExtentType {
		ext := new(proto.InodeExtentKey)
		if err = json.Unmarshal(p.Data, ext); err == nil && proto.IsTinyExtent(ext.ExtentId) {
			log.LogInfof("handleMarkDeletePacket Delete PartitionID(%v)_InodeID(%v)_Extent(%v)_Offset(%v)_Size(%v) from(%v)",
				p.PartitionID, ext.InodeId, p.ExtentID, ext.ExtentOffset, ext.Size, remote)
			if err = partition.MarkDelete(storage.SingleMarker(ext.InodeId, ext.ExtentId, int64(ext.ExtentOffset), int64(ext.Size))); err != nil && log.IsWarnEnabled() {
				log.LogWarnf("partition[%v] mark delete tiny extent[id: %v, offset: %v, size: %v] failed: %v",
					partition.ID(), ext.ExtentId, ext.ExtentOffset, ext.Size, err)
			}
		}
	} else {
		var inode uint64
		if p.Size > 0 && len(p.Data) > 0 {
			ext := new(proto.InodeExtentKey)
			if unmarshalErr := json.Unmarshal(p.Data, ext); unmarshalErr == nil {
				inode = ext.InodeId
			}
		}
		log.LogInfof("handleMarkDeletePacket Delete PartitionID(%v)_InodeID(%v)_Extent(%v) from(%v)",
			p.PartitionID, inode, p.ExtentID, remote)
		if err = partition.MarkDelete(storage.SingleMarker(inode, p.ExtentID, 0, 0)); err != nil && log.IsWarnEnabled() {
			log.LogWarnf("partition[%v] mark delete extent[id: %v, inode: %v] failed: %v",
				partition.ID(), p.ExtentID, inode, err)
		}
	}
	p.PacketOkReply()
	return
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleBatchMarkDeletePacket(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("(%v) error(%v) data(%v)", p.GetUniqueLogId(), err, string(p.Data)))
		}
		p.PacketOkReply()
	}()
	remote := c.RemoteAddr().String()
	partition := p.Object.(*DataPartition)
	toObject := partition.monitorData[proto.ActionBatchMarkDelete].BeforeTp()

	var keys []*proto.InodeExtentKey
	if err = json.Unmarshal(p.Data[0:p.Size], &keys); err != nil {
		return
	}

	var batch = storage.BatchMarker(len(keys))
	for _, key := range keys {
		DeleteLimiterWait()
		if log.IsDebugEnabled() {
			log.LogDebugf("handleBatchMarkDeletePacket Delete PartitionID(%v)_InodeID(%v)_Extent(%v)_Offset(%v)_Size(%v) from(%v)",
				p.PartitionID, key.InodeId, key.ExtentId, key.ExtentOffset, key.Size, remote)
		}
		batch.Add(key.InodeId, key.ExtentId, int64(key.ExtentOffset), int64(key.Size))
	}
	if err = partition.MarkDelete(batch); err != nil {
		return
	}

	toObject.AfterTp(uint64(len(keys)))
	return
}

// Handle OpWrite packet.
func (s *DataNode) handleWritePacket(p *repl.Packet) {
	var err error
	partition := p.Object.(*DataPartition)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	if err = partition.CheckWritable(); err != nil {
		return
	}

	store := partition.ExtentStore()
	if p.ExtentType == proto.TinyExtentType {
		err = store.Write(p.Ctx(), p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data[0:p.Size], p.CRC, storage.AppendWriteType, p.IsSyncWrite())
		s.incDiskErrCnt(p.PartitionID, err, WriteFlag)
		return
	}

	if _, ok := partition.ExtentStore().LoadExtentLockInfo(p.ExtentID); ok {
		err = storage.ExtentLockedError
		return
	}

	if p.Size <= unit.BlockSize {
		err = store.Write(p.Ctx(), p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data[0:p.Size], p.CRC, storage.AppendWriteType, p.IsSyncWrite())
		partition.checkIsPartitionError(err)
	} else {
		size := p.Size
		offset := 0
		for size > 0 {
			if size <= 0 {
				break
			}
			currSize := unit.Min(int(size), unit.BlockSize)
			data := p.Data[offset : offset+currSize]
			crc := crc32.ChecksumIEEE(data)
			err = store.Write(p.Ctx(), p.ExtentID, p.ExtentOffset+int64(offset), int64(currSize), data[0:currSize], crc, storage.AppendWriteType, p.IsSyncWrite())
			partition.checkIsPartitionError(err)
			if err != nil {
				break
			}
			size -= uint32(currSize)
			offset += currSize
		}
	}
	s.incDiskErrCnt(p.PartitionID, err, WriteFlag)
	return
}

func (s *DataNode) handleRandomWritePacket(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*DataPartition)
	if partition.IsRandomWriteDisabled() {
		err = proto.ErrOperationDisabled
		return
	}
	_, isLeader := partition.IsRaftLeader()
	if !isLeader {
		err = raft.ErrNotLeader
		return
	}
	if _, ok := partition.ExtentStore().LoadExtentLockInfo(p.ExtentID); ok {
		err = storage.ExtentLockedError
		return
	}
	err = partition.RandomWriteSubmit(p)
	if err != nil && strings.Contains(err.Error(), raft.ErrNotLeader.Error()) {
		err = raft.ErrNotLeader
		return
	}

	if err == nil && p.ResultCode != proto.OpOk {
		err = storage.TryAgainError
		return
	}
}

func (s *DataNode) handleLockOrUnlockExtent(p *repl.Packet, c net.Conn) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("(%v) error(%v) data(%v)", p.GetUniqueLogId(), err, string(p.Data)))
			p.PackErrorBody(ActionLockOrUnlockExtent, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	remote := c.RemoteAddr().String()
	partition := p.Object.(*DataPartition)
	var extentLockInfo proto.ExtentLockInfo
	if err = json.Unmarshal(p.Data[0:p.Size], &extentLockInfo); err != nil {
		return
	}
	if extentLockInfo.LockStatus == proto.Lock {
		err = partition.ExtentStore().LockExtents(extentLockInfo)
	} else if extentLockInfo.LockStatus == proto.Unlock {
		partition.ExtentStore().UnlockExtents(extentLockInfo)
	}
	log.LogInfof("handleLockOrUnlockExtent PartitionID(%v) LockStatus(%v) extentLockInfo(%v) from(%v) err(%v)",
		p.PartitionID, extentLockInfo.LockStatus, extentLockInfo, remote, err)
	return
}

//func (s *DataNode) handleRandomWritePacketV3(p *repl.Packet) {
//	var err error
//	defer func() {
//		if err != nil {
//			p.PackErrorBody(ActionWrite, err.Error())
//		} else {
//			p.PacketOkReply()
//		}
//	}()
//	partition := p.Object.(*DataPartition)
//	_, isLeader := partition.IsRaftLeader()
//	if !isLeader {
//		err = raft.ErrNotLeader
//		return
//	}
//	err = partition.RandomWriteSubmitV3(p)
//	if err != nil && strings.Contains(err.Error(), raft.ErrNotLeader.Error()) {
//		err = raft.ErrNotLeader
//		return
//	}
//
//	if err == nil && p.ResultCode != proto.OpOk {
//		err = storage.TryAgainError
//		return
//	}
//}

func (s *DataNode) handleStreamReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamRead, err.Error())
		}
	}()

	// 确保Raft服务以及Partition的Raft实例已经启动，否则拒绝服务。
	if s.raftStore == nil {
		err = proto.ErrOperationDisabled
		return
	}
	var partition = p.Object.(*DataPartition)
	if !partition.IsRaftStarted() {
		err = proto.ErrOperationDisabled
		return
	}

	// 检查所请求Partition的一致性模式(Consistency Mode)， 若为标准模式(StandardMode)则仅在当前Partition实例Raft复制组内Leader角色时才提供服务。
	// 标准模式(StandardMode)下Raft采用标准的超半数复制提交机制，这种模式下仅Leader角色可以保证数据的绝对正确。
	// 严格模式(StrictMode)下Raft实例使用了特殊的复制提交机制，数据操作请求被强行要求所有成员全部复制成功才会被提交，所以不需要检查当前实例是否为Leader角色。
	if partition.GetConsistencyMode() == proto.StandardMode {
		if _, err = partition.CheckLeader(); err != nil {
			return
		}
	}

	s.handleExtentRepairReadPacket(p, connect, isRepairRead)
	return
}

func (s *DataNode) handleStreamFollowerReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	s.handleExtentRepairReadPacket(p, connect, isRepairRead)

	return
}

func (s *DataNode) handleExtentRepairReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), connect.RemoteAddr().String(), p.StartT, err))
			if strings.Contains(err.Error(), proto.ConcurrentLimit) {
				log.LogWarnf(logContent)
			} else {
				log.LogErrorf(logContent)
			}
			p.PackErrorBody(ActionStreamRead, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	needReplySize := p.Size
	offset := p.ExtentOffset
	store := partition.ExtentStore()
	if isRepairRead {
		err = multirate.WaitConcurrency(context.Background(), int(proto.OpExtentRepairRead), partition.disk.Path)
		if err != nil {
			err = utilErrors.Trace(err, proto.ConcurrentLimit)
			return
		}
		defer multirate.DoneConcurrency(int(proto.OpExtentRepairRead), partition.disk.Path)
	}

	// isForceRead 函数用来检查读请求包是否有强制读表示，强制读请求会不检查请求所读数据区域是否存在风险
	var isForceRead = len(p.Arg) > 0 && p.Arg[0] == 1

	if !isForceRead && partition.CheckRisk(p.ExtentID, uint64(p.ExtentOffset), uint64(p.Size)) {
		// 正常非强制读请求下，若请求所读数据区域存在风险，则拒绝响应。
		err = proto.ErrOperationDisabled
		return
	}

	action := proto.ActionRead
	if isRepairRead {
		action = proto.ActionRepairRead
	}

	var dataBuffer []byte
	if needReplySize >= unit.ReadBlockSize {
		dataBuffer, _ = proto.Buffers.Get(unit.ReadBlockSize)
		defer proto.Buffers.Put(dataBuffer)
	} else {
		dataBuffer = make([]byte, needReplySize)
	}

	var isStrictModeEnabled = partition.GetConsistencyMode() == proto.StrictMode

	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		reply := repl.NewStreamReadResponsePacket(p.Ctx(), p.ReqID, p.PartitionID, p.ExtentID)
		reply.StartT = p.StartT
		currReadSize := uint32(unit.Min(int(needReplySize), unit.ReadBlockSize))
		err = partition.limit(context.Background(), int(p.Opcode), currReadSize, multirate.FlowNetwork)
		if err != nil {
			return
		}
		err = partition.limit(context.Background(), int(p.Opcode), currReadSize, multirate.FlowDisk)
		if err != nil {
			return
		}
		reply.Data = dataBuffer[:currReadSize]

		reply.ExtentOffset = offset
		p.Size = uint32(currReadSize)
		p.ExtentOffset = offset
		toObject := partition.monitorData[action].BeforeTp()

		err = func() error {
			var storeErr error
			if !isRepairRead {
				tp := exporter.NewModuleTP("StreamRead_StoreRead")
				defer func() {
					tp.Set(storeErr)
				}()
			}
			if isStrictModeEnabled {
				if storeErr = partition.checkAndWaitForPendingActionApplied(reply.ExtentID, offset, int64(currReadSize)); storeErr != nil {
					return storeErr
				}
			}
			reply.CRC, storeErr = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data[0:currReadSize], isRepairRead)
			return storeErr
		}()
		partition.checkIsPartitionError(err)
		p.CRC = reply.CRC
		if err != nil {
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		reply.Opcode = p.Opcode
		p.ResultCode = proto.OpOk

		err = func() error {
			var netErr error
			if !isRepairRead {
				tp := exporter.NewModuleTP("StreamRead_WriteToConn")
				defer func() {
					tp.Set(netErr)
				}()
			}
			netErr = reply.WriteToConn(connect, proto.WriteDeadlineTime)
			return netErr
		}()
		toObject.AfterTp(uint64(currReadSize))
		if err != nil {
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
			log.LogErrorf(logContent)
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
		log.LogReadf(logContent)
	}
	p.PacketOkReply()

	return
}

func (s *DataNode) handlePacketToGetAllWatermarks(p *repl.Packet) {
	var (
		buf       []byte
		fInfoList []storage.ExtentInfoBlock
		err       error
	)
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetAllExtentWatermarks, err.Error())
		}
	}()
	if p.ExtentType == proto.NormalExtentType {
		if !store.IsFinishLoad() {
			err = storage.PartitionIsLoaddingErr
			return
		}
		fInfoList, err = store.GetAllWatermarks(p.ExtentType, storage.NormalExtentFilter())
	} else {
		extents := make([]uint64, 0)
		err = json.Unmarshal(p.Data, &extents)
		if err == nil {
			fInfoList, err = store.GetAllWatermarks(p.ExtentType, storage.TinyExtentFilter(extents))
		}
	}
	buf, err = json.Marshal(fInfoList)
	if err != nil {
		return
	}
	p.PacketOkWithBody(buf)
	return
}

// V2使用二进制编解码
func (s *DataNode) handlePacketToGetAllWatermarksV2(p *repl.Packet) {
	var (
		err  error
		data []byte
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetAllExtentWatermarksV2, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if p.ExtentType == proto.NormalExtentType {
		if !store.IsFinishLoad() {
			err = storage.PartitionIsLoaddingErr
			return
		}
		_, data, err = store.GetAllWatermarksWithByteArr(p.ExtentType, storage.NormalExtentFilter())
	} else {
		var extentIDs = make([]uint64, 0, len(p.Data)/8)
		var extentID uint64
		var reader = bytes.NewReader(p.Data)
		for {
			err = binary.Read(reader, binary.BigEndian, &extentID)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return
			}
			extentIDs = append(extentIDs, extentID)
		}
		_, data, err = store.GetAllWatermarksWithByteArr(p.ExtentType, storage.TinyExtentFilter(extentIDs))
	}
	if err != nil {
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (s *DataNode) handlePacketToGetAllWatermarksV3(p *repl.Packet) {
	var (
		err        error
		watermarks []byte
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetAllExtentWatermarksV3, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if p.ExtentType == proto.NormalExtentType {
		if !store.IsFinishLoad() {
			err = storage.PartitionIsLoaddingErr
			return
		}
		_, watermarks, err = store.GetAllWatermarksWithByteArr(p.ExtentType, storage.NormalExtentFilter())
	} else {
		var extentIDs = make([]uint64, 0, len(p.Data)/8)
		var extentID uint64
		var reader = bytes.NewReader(p.Data)
		for {
			err = binary.Read(reader, binary.BigEndian, &extentID)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return
			}
			extentIDs = append(extentIDs, extentID)
		}
		_, watermarks, err = store.GetAllWatermarksWithByteArr(p.ExtentType, storage.TinyExtentFilter(extentIDs))
	}
	if err != nil {
		return
	}
	var data = make([]byte, len(watermarks)+8)
	binary.BigEndian.PutUint64(data[:8], store.GetBaseExtentID())
	copy(data[8:], watermarks)
	p.PacketOkWithBody(data)
	return
}

func (s *DataNode) handleFingerprintPacket(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetExtentFingerprint, err.Error())
		}
	}()
	var (
		req         *repl.FingerprintRequest
		partition   = p.Object.(*DataPartition)
		store       = partition.ExtentStore()
		fingerprint storage.Fingerprint
	)

	if req, err = repl.ParseFingerprintPacket(p); err != nil {
		return
	}

	var isIssue bool
	if isIssue = partition.CheckRisk(p.ExtentID, uint64(p.ExtentOffset), uint64(p.Size)); isIssue && !req.Force {
		// 正常非强制读请求下，若请求所读数据区域存在风险，则拒绝响应。
		err = proto.ErrOperationDisabled
		return
	}

	if !req.Force && partition.CheckRisk(p.ExtentID, uint64(p.ExtentOffset), uint64(p.Size)) {
		// 正常非强制读请求下，若请求所读数据区域存在风险，则拒绝响应。
		err = proto.ErrOperationDisabled
		return
	}

	if fingerprint, err = store.Fingerprint(p.ExtentID, req.Offset, req.Size, isIssue); err != nil {
		return
	}
	var data = fingerprint.EncodeBinary()
	p.PacketOkWithBody(data)
	return
}

func (s *DataNode) handlePacketToGetAllExtentInfo(p *repl.Packet) {
	var (
		err  error
		data []byte
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetAllExtentInfo, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if !store.IsFinishLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	data, err = store.GetAllExtentInfoWithByteArr(storage.ExtentFilterForValidateCRC())
	if err != nil {
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (s *DataNode) writeEmptyPacketOnTinyExtentRepairRead(reply *repl.Packet, newOffset, currentOffset int64, connect net.Conn) (replySize int64, err error) {
	replySize = newOffset - currentOffset
	reply.Data = make([]byte, 0)
	reply.Size = 0
	reply.CRC = crc32.ChecksumIEEE(reply.Data)
	reply.ResultCode = proto.OpOk
	reply.ExtentOffset = currentOffset
	reply.Arg[0] = EmptyResponse
	binary.BigEndian.PutUint64(reply.Arg[1:9], uint64(replySize))
	err = reply.WriteToConn(connect, proto.WriteDeadlineTime)
	if replySize >= math.MaxUint32 {
		reply.Size = math.MaxUint32
	} else {
		reply.Size = uint32(replySize)
	}
	//redirect kernelOffset as crc of Arg
	reply.KernelOffset = uint64(crc32.ChecksumIEEE(reply.Arg))
	logContent := fmt.Sprintf("action[write empty repair packet] %v.",
		reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
	log.LogReadf(logContent)

	return
}

func (s *DataNode) attachAvaliSizeOnTinyExtentRepairRead(reply *repl.Packet, avaliSize uint64) {
	binary.BigEndian.PutUint64(reply.Arg[9:TinyExtentRepairReadResponseArgLen], avaliSize)
}

// Handle handleTinyExtentRepairRead packet.
func (s *DataNode) handleTinyExtentRepairRead(request *repl.Packet, connect net.Conn) {
	var (
		err                 error
		needReplySize       int64
		tinyExtentFinfoSize uint64
	)

	defer func() {
		if err != nil {
			request.PackErrorBody(ActionStreamReadTinyExtentRepair, err.Error())
		}
	}()
	if !proto.IsTinyExtent(request.ExtentID) {
		err = fmt.Errorf("unavali extentID(%v)", request.ExtentID)
		return
	}

	partition := request.Object.(*DataPartition)

	err = multirate.WaitConcurrency(context.Background(), int(proto.OpTinyExtentRepairRead), partition.disk.Path)
	if err != nil {
		err = utilErrors.Trace(err, proto.ConcurrentLimit)
		return
	}
	defer multirate.DoneConcurrency(int(proto.OpTinyExtentRepairRead), partition.disk.Path)

	store := partition.ExtentStore()
	tinyExtentFinfoSize, err = store.TinyExtentGetFinfoSize(request.ExtentID)
	if err != nil {
		return
	}
	offset := request.ExtentOffset
	needReplySize = int64(tinyExtentFinfoSize - uint64(request.ExtentOffset))
	avaliReplySize := uint64(needReplySize)

	var (
		newOffset, newEnd int64
	)
	for {
		if needReplySize <= 0 {
			break
		}
		reply := repl.NewTinyExtentStreamReadResponsePacket(request.Ctx(), request.ReqID, request.PartitionID, request.ExtentID)
		reply.ArgLen = TinyExtentRepairReadResponseArgLen
		reply.Arg = make([]byte, TinyExtentRepairReadResponseArgLen)
		s.attachAvaliSizeOnTinyExtentRepairRead(reply, avaliReplySize)
		newOffset, newEnd, err = store.TinyExtentAvaliOffset(request.ExtentID, offset)
		if err != nil {
			return
		}
		if newOffset > offset {
			var (
				replySize int64
			)
			if replySize, err = s.writeEmptyPacketOnTinyExtentRepairRead(reply, newOffset, offset, connect); err != nil {
				return
			}
			needReplySize -= replySize
			offset += replySize
			continue
		}
		currNeedReplySize := newEnd - newOffset
		currReadSize := uint32(unit.Min(int(currNeedReplySize), unit.ReadBlockSize))
		err = partition.limit(context.Background(), int(proto.OpTinyExtentRepairRead), currReadSize, multirate.FlowDisk)
		if err != nil {
			return
		}
		if currReadSize == unit.ReadBlockSize {
			reply.Data, _ = proto.Buffers.Get(unit.ReadBlockSize)
		} else {
			reply.Data = make([]byte, currReadSize)
		}
		reply.ExtentOffset = offset
		var tpObject = partition.monitorData[proto.ActionRepairRead].BeforeTp()
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data, false)
		if err != nil {
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		reply.Size = currReadSize
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
			connect.Close()
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		tpObject.AfterTp(uint64(currReadSize))
		needReplySize -= int64(currReadSize)
		offset += int64(currReadSize)
		if currReadSize == unit.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
		log.LogReadf(logContent)
	}

	request.PacketOkReply()
	return
}

func (s *DataNode) handleTinyExtentAvaliRead(request *repl.Packet, connect net.Conn) {
	var (
		err                 error
		needReplySize       int64
		tinyExtentFinfoSize uint64
	)

	defer func() {
		if err != nil {
			request.PackErrorBody(ActionStreamReadTinyExtentAvali, err.Error())
		}
	}()
	if !proto.IsTinyExtent(request.ExtentID) {
		err = fmt.Errorf("unavali extentID (%v)", request.ExtentID)
		return
	}

	partition := request.Object.(*DataPartition)
	store := partition.ExtentStore()
	tinyExtentFinfoSize, err = store.TinyExtentGetFinfoSize(request.ExtentID)
	if err != nil {
		return
	}
	needReplySize = int64(request.Size)
	offset := request.ExtentOffset
	if uint64(request.ExtentOffset)+uint64(request.Size) > tinyExtentFinfoSize {
		needReplySize = int64(tinyExtentFinfoSize - uint64(request.ExtentOffset))
	}
	avaliReplySize := uint64(needReplySize)

	var (
		newOffset, newEnd int64
	)
	for {
		if needReplySize <= 0 {
			break
		}
		reply := repl.NewTinyExtentStreamReadResponsePacket(request.Ctx(), request.ReqID, request.PartitionID, request.ExtentID)
		reply.Opcode = proto.OpTinyExtentAvaliRead
		reply.ArgLen = TinyExtentRepairReadResponseArgLen
		reply.Arg = make([]byte, TinyExtentRepairReadResponseArgLen)
		s.attachAvaliSizeOnTinyExtentRepairRead(reply, avaliReplySize)
		newOffset, newEnd, err = store.TinyExtentAvaliOffset(request.ExtentID, offset)
		if err != nil {
			return
		}
		if newOffset > offset {
			replySize := newOffset - offset
			offset += replySize
			continue
		}
		currNeedReplySize := newEnd - newOffset
		if currNeedReplySize <= 0 {
			err = fmt.Errorf("ExtentID(%v) offset(%v) currNeedReplySize(%v) <= 0", request.ExtentID, offset, currNeedReplySize)
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
			log.LogErrorf(logContent)
			break
		}
		currReadSize := uint32(unit.Min(int(currNeedReplySize), unit.ReadBlockSize))
		err = partition.limit(context.Background(), int(proto.OpTinyExtentAvaliRead), currReadSize, multirate.FlowDisk)
		if err != nil {
			return
		}
		if currReadSize == unit.ReadBlockSize {
			reply.Data, _ = proto.Buffers.Get(unit.ReadBlockSize)
		} else {
			reply.Data = make([]byte, currReadSize)
		}
		reply.ExtentOffset = offset
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data, false)
		if err != nil {
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		reply.Size = currReadSize
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
			connect.Close()
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		needReplySize -= int64(currReadSize)
		offset += int64(currReadSize)
		if currReadSize == unit.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
		log.LogReadf(logContent)
	}

	request.PacketOkReply()
	return
}

func (s *DataNode) handlePacketToReadTinyDeleteRecordFile(p *repl.Packet, connect *net.TCPConn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamReadTinyDeleteRecord, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	localTinyDeleteFileSize, err := store.LoadTinyDeleteFileOffset()
	if err != nil {
		return
	}
	needReplySize := localTinyDeleteFileSize - p.ExtentOffset
	offset := p.ExtentOffset
	reply := repl.NewReadTinyDeleteRecordResponsePacket(p.Ctx(), p.ReqID, p.PartitionID)
	reply.StartT = time.Now().UnixNano()
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		currReadSize := uint32(unit.Min(int(needReplySize), MaxSyncTinyDeleteBufferSize))
		reply.Data = make([]byte, currReadSize)
		reply.ExtentOffset = offset
		reply.CRC, err = store.ReadTinyDeleteRecords(offset, int64(currReadSize), reply.Data)
		if err != nil {
			err = fmt.Errorf(ActionStreamReadTinyDeleteRecord+" localTinyDeleteRecordSize(%v) offset(%v)"+
				" currReadSize(%v) err(%v)", localTinyDeleteFileSize, offset, currReadSize, err)
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
			return
		}
		needReplySize -= int64(currReadSize)
		offset += int64(currReadSize)
	}
	p.PacketOkReply()

	return
}

// Handle OpNotifyReplicasToRepair packet.
func (s *DataNode) handlePacketToNotifyExtentRepair(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRepair, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	mf := new(DataPartitionRepairTask)
	err = json.Unmarshal(p.Data, mf)
	if err != nil {
		return
	}
	partition.DoExtentStoreRepairOnFollowerDisk(mf)
	p.PacketOkReply()
	return
}

// Handle OpBroadcastMinAppliedID
func (s *DataNode) handleBroadcastMinAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	minAppliedID := binary.BigEndian.Uint64(p.Data[0:8])
	if minAppliedID > 0 {
		partition.TruncateRaftWAL(minAppliedID)
	}
	p.PacketOkReply()
	return
}

// Handle handlePacketToGetAppliedID packet.
func (s *DataNode) handlePacketToGetAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	appliedID := partition.GetAppliedID()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedID)
	p.PacketOkWithBody(buf)
	p.AddMesgLog(fmt.Sprintf("_AppliedID(%v)", appliedID))
	return
}

func (s *DataNode) handlePacketToGetPersistedAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	persistedAppliedID := partition.GetPersistedAppliedID()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, persistedAppliedID)
	p.PacketOkWithBody(buf)
	p.AddMesgLog(fmt.Sprintf("_PersistedAppliedID(%v)", persistedAppliedID))
	return
}

func (s *DataNode) handlePacketToGetPartitionSize(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetPartitionSize, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	if !partition.ExtentStore().IsFinishLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	usedSize := partition.extentStore.StoreSizeExtentID(p.ExtentID)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(usedSize))
	p.AddMesgLog(fmt.Sprintf("partitionSize_(%v)", usedSize))
	p.PacketOkWithBody(buf)

	return
}

func (s *DataNode) handlePacketToGetMaxExtentIDAndPartitionSize(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(GetMaxExtentIDAndPartitionSize, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	if !partition.ExtentStore().IsFinishLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	maxExtentID, totalPartitionSize := partition.extentStore.GetMaxExtentIDAndPartitionSize()
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(maxExtentID))
	binary.BigEndian.PutUint64(buf[8:16], totalPartitionSize)
	p.PacketOkWithBody(buf)

	return
}

func (s *DataNode) handlePacketToDecommissionDataPartition(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.DataPartitionDecommissionRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDecommissionPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = fmt.Errorf("partition %v not exsit", req.PartitionId)
		return
	}
	p.PartitionID = req.PartitionId

	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		err = raft.ErrNotLeader
		return
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = utilErrors.NewErrorf("[opOfflineDataPartition]: AddPeer(%v) same withRemovePeer(%v)", req.AddPeer, req.RemovePeer)
		return
	}
	if req.AddPeer.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfAddNode, raftProto.Peer{ID: req.AddPeer.ID}, reqData)
		if err != nil {
			return
		}
	}
	_, err = dp.ChangeRaftMember(raftProto.ConfRemoveNode, raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		return
	}
	return
}

func (s *DataNode) handlePacketToAddDataPartitionRaftMember(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.AddDataPartitionRaftMemberRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionAddDataPartitionRaftMember, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.PartitionID = req.PartitionId
	if dp.IsExistReplica(req.AddPeer.Addr) {
		log.LogInfof("handlePacketToAddDataPartitionRaftMember recive MasterCommand: %v "+
			"addRaftAddr(%v) has exsit", string(reqData), req.AddPeer.Addr)
		return
	}
	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}

	if req.AddPeer.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfAddNode, raftProto.Peer{ID: req.AddPeer.ID}, reqData)
		if err != nil {
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToRemoveDataPartitionRaftMember(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.RemoveDataPartitionRaftMemberRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRemoveDataPartitionRaftMember, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}
	reqData, err = json.Marshal(adminTask.Request)
	p.AddMesgLog(string(reqData))
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	req.ReserveResource = adminTask.ReserveResource
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		return
	}
	p.PartitionID = req.PartitionId

	if !req.RaftOnly && !dp.IsExistReplica(req.RemovePeer.Addr) {
		log.LogInfof("handlePacketToRemoveDataPartitionRaftMember recive MasterCommand: %v "+
			"RemoveRaftPeer(%v) has not exsit", string(reqData), req.RemovePeer.Addr)
		return
	}

	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}
	if !req.RaftOnly {
		if err = dp.CanRemoveRaftMember(req.RemovePeer); err != nil {
			return
		}
	}
	if req.RemovePeer.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfRemoveNode, raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
		if err != nil {
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToResetDataPartitionRaftMember(p *repl.Packet) {
	var (
		err       error
		reqData   []byte
		isUpdated bool
		req       = &proto.ResetDataPartitionRaftMemberRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionResetDataPartitionRaftMember, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	p.AddMesgLog(string(reqData))
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}

	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = fmt.Errorf("partition %v not exsit", req.PartitionId)
		return
	}
	p.PartitionID = req.PartitionId
	for _, peer := range req.NewPeers {
		if !dp.IsExistReplica(peer.Addr) {
			log.LogErrorf("handlePacketToResetDataPartitionRaftMember recive MasterCommand: %v "+
				"ResetRaftPeer(%v) has not exsit", string(reqData), peer.Addr)
			return
		}
		if peer.ID == 0 {
			log.LogErrorf("handlePacketToResetDataPartitionRaftMember recive MasterCommand: %v "+
				"Peer ID(%v) not valid", string(reqData), peer.ID)
			return
		}
	}
	if isUpdated, err = dp.resetRaftNode(req); err != nil {
		return
	}
	if isUpdated {
		if err = dp.ChangeCreateType(proto.NormalCreateDataPartition); err != nil {
			log.LogErrorf("handlePacketToResetDataPartitionRaftMember dp(%v) PersistMetadata err(%v).", dp.ID(), err)
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToAddDataPartitionRaftLearner(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.AddDataPartitionRaftLearnerRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionAddDataPartitionRaftLearner, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.PartitionID = req.PartitionId
	if dp.IsExistReplica(req.AddLearner.Addr) && dp.IsExistLearner(req.AddLearner) {
		log.LogInfof("handlePacketToAddDataPartitionRaftLearner receive MasterCommand: %v "+
			"addRaftLearnerAddr(%v) has exist", string(reqData), req.AddLearner.Addr)
		return
	}
	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}

	if req.AddLearner.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfAddLearner, raftProto.Peer{ID: req.AddLearner.ID}, reqData)
		if err != nil {
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToPromoteDataPartitionRaftLearner(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.PromoteDataPartitionRaftLearnerRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionPromoteDataPartitionRaftLearner, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.PartitionID = req.PartitionId
	if !dp.IsExistReplica(req.PromoteLearner.Addr) || !dp.IsExistLearner(req.PromoteLearner) {
		log.LogInfof("handlePacketToPromoteDataPartitionRaftLearner receive MasterCommand: %v "+
			"promoteRaftLearnerAddr(%v) has exist", string(reqData), req.PromoteLearner.Addr)
		return
	}
	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}

	if req.PromoteLearner.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfPromoteLearner, raftProto.Peer{ID: req.PromoteLearner.ID}, reqData)
		if err != nil {
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToDataPartitionTryToLeader(p *repl.Packet) {
	var (
		err error
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDataPartitionTryToLeader, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	dp := s.space.Partition(p.PartitionID)
	if dp == nil {
		err = fmt.Errorf("partition %v not exsit", p.PartitionID)
		return
	}

	if dp.raftPartition.IsRaftLeader() {
		return
	}
	err = dp.raftPartition.TryToLeader(dp.ID())
	return
}

func (s *DataNode) handlePacketToSyncDataPartitionReplicas(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionSyncDataPartitionReplicas, err.Error())
		}
	}()
	task := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, task); err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	if task.OpCode != proto.OpSyncDataPartitionReplicas {
		err = fmt.Errorf("from master Task(%v) failed,error unavali opcode(%v)", task.ToString(), task.OpCode)
		return
	}
	request := &proto.SyncDataPartitionReplicasRequest{}
	bytes, err := json.Marshal(task.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(bytes, request); err != nil {
		return
	}
	s.space.SyncPartitionReplicas(request.PartitionId, request.PersistenceHosts)
	p.PacketOkReply()
	return

}

const (
	forwardToRaftLeaderTimeOut = 60 * 2
)

func (s *DataNode) forwardToRaftLeader(dp *DataPartition, p *repl.Packet) (ok bool, err error) {
	var (
		conn       *net.TCPConn
		leaderAddr string
	)

	if leaderAddr, ok = dp.IsRaftLeader(); ok {
		return
	}

	// return NoLeaderError if leaderAddr is nil
	if leaderAddr == "" {
		err = storage.NoLeaderError
		return
	}

	// forward the packet to the leader if local one is not the leader
	conn, err = gConnPool.GetConnect(leaderAddr)
	if err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn, proto.WriteDeadlineTime)
	if err != nil {
		return
	}
	if err = p.ReadFromConn(conn, forwardToRaftLeaderTimeOut); err != nil {
		return
	}
	if p.ResultCode != proto.OpOk {
		err = utilErrors.NewErrorf("forwardToRaftLeader error msg: %v", p.GetOpMsgWithReqAndResult())
		return
	}
	return
}

func (s *DataNode) forwardToRaftLeaderWithTimeOut(dp *DataPartition, p *repl.Packet) (ok bool, err error) {
	var (
		conn       *net.TCPConn
		leaderAddr string
	)

	if leaderAddr, ok = dp.IsRaftLeader(); ok {
		return
	}

	// return NoLeaderError if leaderAddr is nil
	if leaderAddr == "" {
		err = storage.NoLeaderError
		return
	}

	// forward the packet to the leader if local one is not the leader
	conn, err = gConnPool.GetConnect(leaderAddr)
	if err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn, proto.WriteDeadlineTime)
	if err != nil {
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return
	}

	return
}

func (s *DataNode) responseHeartbeatPb(task *proto.AdminTask) {
	var err error
	request := &proto.HeartBeatRequest{}
	taskPb := task.ConvertToPb()
	taskPb.DataNodeResponse = &proto.DataNodeHeartbeatResponsePb{}
	s.buildHeartBeatResponsePb(taskPb.DataNodeResponse)

	if task.OpCode == proto.OpDataNodeHeartbeat {
		marshaled, _ := json.Marshal(task.Request)
		_ = json.Unmarshal(marshaled, request)
		taskPb.Request = request.ConvertToPb()
		taskPb.DataNodeResponse.Status = proto.TaskSucceeds
	} else {
		taskPb.DataNodeResponse.Status = proto.TaskFailed
		err = fmt.Errorf("illegal opcode")
		taskPb.DataNodeResponse.Result = err.Error()
	}
	if err = MasterClient.NodeAPI().ResponseHeartBeatTaskPb(taskPb); err != nil {
		err = utilErrors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
		log.LogErrorf(err.Error())
		return
	}
}

func (s *DataNode) responseHeartbeat(task *proto.AdminTask) {
	var err error
	request := &proto.HeartBeatRequest{}
	resp := &proto.DataNodeHeartbeatResponse{}
	s.buildHeartBeatResponse(resp)

	if task.OpCode == proto.OpDataNodeHeartbeat {
		marshaled, _ := json.Marshal(task.Request)
		_ = json.Unmarshal(marshaled, request)
		task.Request = request
		resp.Status = proto.TaskSucceeds
	} else {
		resp.Status = proto.TaskFailed
		err = fmt.Errorf("illegal opcode")
		resp.Result = err.Error()
	}
	task.Response = resp
	if err = MasterClient.NodeAPI().ResponseDataNodeTask(task); err != nil {
		err = utilErrors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
		log.LogErrorf(err.Error())
		return
	}
}

func BeforeTpMonitor(p *repl.Packet) *statistics.TpObject {
	var monitorOp int
	switch p.Opcode {
	case proto.OpWrite, proto.OpSyncWrite:
		monitorOp = proto.ActionAppendWrite
	case proto.OpMarkDelete:
		monitorOp = proto.ActionMarkDelete
	default:
		return nil
	}
	return p.Object.(*DataPartition).monitorData[monitorOp].BeforeTp()
}

func (s *DataNode) checkLimit(pkg *repl.Packet) (err error) {
	if isStreamOp(int(pkg.Opcode)) {
		return nil
	}
	return pkg.Object.(*DataPartition).limit(context.Background(), int(pkg.Opcode), pkg.Size, multirate.FlowNetwork)
}

func isStreamOp(op int) bool {
	return op == proto.OpExtentRepairWrite_ || op == int(proto.OpStreamRead) || op == int(proto.OpStreamFollowerRead) || op == int(proto.OpTinyExtentRepairRead) || op == int(proto.OpExtentRepairRead)
}

// post
func (s *DataNode) Post(p *repl.Packet) error {
	// 标记稍后是否由复制协议自动回写响应包
	// 除成功的读请求外，均需要复制协议回写响应。
	// 处理成功的读请求回写响应在Operate阶段由相关处理函数处理，不需要复制协议回写响应。
	p.NeedReply = !(p.IsReadOperation() && !p.IsErrPacket())
	s.cleanupPkt(p)
	s.addMetrics(p)
	return nil
}

func (s *DataNode) cleanupPkt(p *repl.Packet) {
	if p.IsMasterCommand() {
		return
	}
	if !p.IsLeaderPacket() {
		return
	}
	s.releaseExtent(p)
}

func (s *DataNode) releaseExtent(p *repl.Packet) {
	if p == nil || !proto.IsTinyExtent(p.ExtentID) || p.ExtentID <= 0 || atomic.LoadInt32(&p.IsReleased) == IsReleased {
		return
	}
	if !p.IsTinyExtentType() || !p.IsLeaderPacket() || !p.IsWriteOperation() || !p.IsForwardPkt() {
		return
	}
	if p.Object == nil {
		return
	}
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if p.IsErrPacket() {
		store.SendToBrokenTinyExtentC(p.ExtentID)
	} else {
		store.SendToAvailableTinyExtentC(p.ExtentID)
	}
	atomic.StoreInt32(&p.IsReleased, IsReleased)
}

func (s *DataNode) addMetrics(p *repl.Packet) {
	if p.IsMasterCommand() {
		return
	}
	p.AfterTp()
}

// prepare
func (s *DataNode) Prepare(p *repl.Packet, remoteAddr string) (err error) {
	defer func() {
		p.SetPacketHasPrepare(remoteAddr)
		if err != nil {
			p.PackErrorBody(repl.ActionPreparePkt, err.Error())
		}
	}()
	if p.IsMasterCommand() {
		return
	}
	if err = s.checkReplInfo(p); err != nil {
		return
	}
	p.BeforeTp(s.clusterID)
	err = s.checkStoreMode(p)
	if err != nil {
		return
	}
	if err = s.checkCrc(p); err != nil {
		return
	}
	if err = s.checkPartition(p); err != nil {
		return
	}
	if err = s.checkLimit(p); err != nil {
		return
	}
	// For certain packet, we need to add some additional extent information.
	if err = s.addExtentInfo(p); err != nil {
		return
	}

	return
}

func (s *DataNode) checkStoreMode(p *repl.Packet) (err error) {
	if p.ExtentType == proto.TinyExtentType || p.ExtentType == proto.NormalExtentType {
		return nil
	}
	return ErrIncorrectStoreType
}

func (s *DataNode) checkCrc(p *repl.Packet) (err error) {
	if !p.IsWriteOperation() {
		return
	}
	crc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if crc != p.CRC {
		return storage.CrcMismatchError
	}

	return
}

func (s *DataNode) checkPartition(p *repl.Packet) (err error) {
	dp := s.space.Partition(p.PartitionID)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.Object = dp
	if p.IsWriteOperation() || p.IsCreateExtentOperation() {
		if err = dp.CheckWritable(); err != nil {
			return
		}
	}
	return
}

func (s *DataNode) addExtentInfo(p *repl.Packet) error {
	partition := p.Object.(*DataPartition)
	store := p.Object.(*DataPartition).ExtentStore()
	var (
		extentID uint64
		err      error
	)
	if p.IsLeaderPacket() && p.IsTinyExtentType() && p.IsWriteOperation() {
		extentID, err = store.GetAvailableTinyExtent()
		if err != nil {
			return fmt.Errorf("addExtentInfo partition %v GetAvailableTinyExtent error %v", p.PartitionID, err.Error())
		}
		p.ExtentID = extentID
		p.ExtentOffset, err = store.GetTinyExtentOffset(extentID)
		if err != nil {
			return fmt.Errorf("addExtentInfo partition %v  %v GetTinyExtentOffset error %v", p.PartitionID, extentID, err.Error())
		}
	} else if p.IsLeaderPacket() && p.IsCreateExtentOperation() {
		if partition.GetExtentCount() >= storage.MaxExtentCount*3 {
			return fmt.Errorf("addExtentInfo partition %v has reached maxExtentId", p.PartitionID)
		}
		p.ExtentID, err = partition.AllocateExtentID()
		if err != nil {
			return fmt.Errorf("addExtentInfo partition %v alloc NextExtentId error %v", p.PartitionID, err)
		}
	} else if p.IsLeaderPacket() && p.IsMarkDeleteExtentOperation() && p.IsTinyExtentType() {
		record := new(proto.InodeExtentKey)
		if err := json.Unmarshal(p.Data[:p.Size], record); err != nil {
			return fmt.Errorf("addExtentInfo failed %v", err.Error())
		}
		p.Data, _ = json.Marshal(record)
		p.Size = uint32(len(p.Data))
		p.OrgBuffer = p.Data
	}

	return nil
}

func (s *DataNode) checkReplInfo(p *repl.Packet) (err error) {
	if p.IsLeaderPacket() && len(p.GetFollowers()) == 0 {
		err = fmt.Errorf("checkReplInfo: leader write packet without follower address")
		return
	}
	return
}

// handler
func (s *DataNode) getDiskAPI(w http.ResponseWriter, r *http.Request) {
	disks := make([]interface{}, 0)
	for _, diskItem := range s.space.GetDisks() {
		disk := &struct {
			Path        string `json:"path"`
			Total       uint64 `json:"total"`
			Used        uint64 `json:"used"`
			Available   uint64 `json:"available"`
			Unallocated uint64 `json:"unallocated"`
			Allocated   uint64 `json:"allocated"`
			Status      int    `json:"status"`
			RestSize    uint64 `json:"restSize"`
			Partitions  int    `json:"partitions"`

			// Limits
			ExecutingRepairTask          uint64 `json:"executing_repair_task"`
			FixTinyDeleteRecordLimit     uint64 `json:"fix_tiny_delete_record_limit"`
			ExecutingFixTinyDeleteRecord uint64 `json:"executing_fix_tiny_delete_record"`
		}{
			Path:        diskItem.Path,
			Total:       diskItem.Total,
			Used:        diskItem.Used,
			Available:   diskItem.Available,
			Unallocated: diskItem.Unallocated,
			Allocated:   diskItem.Allocated,
			Status:      diskItem.Status,
			RestSize:    diskItem.ReservedSpace,
			Partitions:  diskItem.PartitionCount(),

			FixTinyDeleteRecordLimit:     diskItem.fixTinyDeleteRecordLimit,
			ExecutingFixTinyDeleteRecord: diskItem.executingFixTinyDeleteRecord,
			ExecutingRepairTask:          diskItem.executingRepairTask,
		}
		disks = append(disks, disk)
	}
	diskReport := &struct {
		Disks []interface{} `json:"disks"`
		Zone  string        `json:"zone"`
	}{
		Disks: disks,
		Zone:  s.zoneName,
	}
	s.buildSuccessResp(w, diskReport)
}

func (s *DataNode) getStatAPI(w http.ResponseWriter, r *http.Request) {
	response := &proto.DataNodeHeartbeatResponse{}
	s.buildHeartBeatResponse(response)

	s.buildSuccessResp(w, response)
}

func (s *DataNode) setAutoRepairStatus(w http.ResponseWriter, r *http.Request) {
	const (
		paramAutoRepair = "autoRepair"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	autoRepair, err := strconv.ParseBool(r.FormValue(paramAutoRepair))
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramAutoRepair, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	AutoRepairStatus = autoRepair
	s.buildSuccessResp(w, autoRepair)
}

/*
 * release the space of mark delete data partitions of the whole node.
 */
func (s *DataNode) releasePartitions(w http.ResponseWriter, r *http.Request) {
	const (
		paramAuthKey     = "key"
		paramKeepTimeSec = "keepTimeSec"

		defaultKeepTImeSec = 60 * 60 * 24 // 24 Hours
	)
	var (
		successVols []string
		failedVols  []string
		failedDisks []string
	)
	successVols = make([]string, 0)
	failedVols = make([]string, 0)
	failedDisks = make([]string, 0)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	key := r.FormValue(paramAuthKey)
	if !matchKey(key) {
		err := fmt.Errorf("auth key not match: %v", key)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	var keepTimeSec = defaultKeepTImeSec
	if keepTimeSecVal := r.FormValue(paramKeepTimeSec); len(keepTimeSecVal) > 0 {
		if pared, err := strconv.Atoi(keepTimeSecVal); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse param %v failed: %v", paramKeepTimeSec, err))
			return
		} else {
			keepTimeSec = pared
		}
	}

	// Format: expired_datapartition_{ID}_{Capacity}_{Timestamp}
	// Regexp: ^expired_datapartition_(\d)+_(\d)+_(\d)+$
	var regexpExpiredPartitionDirName = regexp.MustCompile("^expired_datapartition_(\\d)+_(\\d)+_(\\d)+$")
	var keepTimeDuring = time.Second * time.Duration(keepTimeSec)
	var now = time.Now()

	for _, d := range s.space.disks {
		fList, err := ioutil.ReadDir(d.Path)
		if err != nil {
			failedDisks = append(failedDisks, d.Path)
			continue
		}

		for _, fInfo := range fList {
			if !fInfo.IsDir() {
				continue
			}
			var filename = fInfo.Name()
			if !regexpExpiredPartitionDirName.MatchString(filename) {
				continue
			}
			var parts = strings.Split(filename, "_")
			var timestamp uint64
			if timestamp, err = strconv.ParseUint(parts[len(parts)-1], 10, 64); err != nil {
				failedVols = append(failedVols, d.Path+":"+fInfo.Name())
				continue
			}
			var expiredTime = time.Unix(int64(timestamp), 0)
			if expiredTime.Add(keepTimeDuring).After(now) {
				continue
			}
			err = os.RemoveAll(d.Path + "/" + fInfo.Name())
			if err != nil {
				failedVols = append(failedVols, d.Path+":"+fInfo.Name())
				continue
			}
			successVols = append(successVols, d.Path+":"+fInfo.Name())
		}
	}
	s.buildSuccessResp(w, fmt.Sprintf("release partitions, success partitions: %v, failed partitions: %v, failed disks: %v", successVols, failedVols, failedDisks))
}

func (s *DataNode) restorePartitions(w http.ResponseWriter, r *http.Request) {
	const (
		paramAuthKey = "key"
		paramIdKey   = "id"
	)
	var (
		err         error
		all         bool
		successDps  []uint64
		failedDisks []string
		failedDps   []uint64
	)
	ids := make(map[uint64]bool)
	key := r.FormValue(paramAuthKey)
	if !matchKey(key) {
		err = fmt.Errorf("auth key not match: %v", key)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	idVal := r.FormValue(paramIdKey)
	if idVal == "all" {
		all = true
	} else {
		allId := strings.Split(idVal, ",")
		for _, val := range allId {
			id, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				continue
			}
			ids[id] = true
		}
	}
	// Format: expired_datapartition_{ID}_{Capacity}_{Timestamp}
	// Regexp: ^expired_datapartition_(\d)+_(\d)+_(\d)+$
	regexpExpiredPartitionDirName := regexp.MustCompile("^expired_datapartition_(\\d)+_(\\d)+_(\\d)+$")
	for _, d := range s.space.disks {
		var entries []fs.DirEntry
		entries, err = os.ReadDir(d.Path)
		if err != nil {
			failedDisks = append(failedDisks, d.Path)
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			filename := entry.Name()
			if !regexpExpiredPartitionDirName.MatchString(filename) {
				continue
			}
			parts := strings.Split(filename, "_")
			dpid, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				failedDps = append(failedDps, dpid)
				continue
			}
			if !all {
				if _, ok := ids[dpid]; !ok {
					continue
				}
			}
			newname := strings.Join(parts[1:4], "_")
			err = os.Rename(d.Path+"/"+filename, d.Path+"/"+newname)
			if err != nil {
				failedDps = append(failedDps, dpid)
				continue
			}
			err = s.space.LoadPartition(d, dpid, newname)
			if err != nil {
				failedDps = append(failedDps, dpid)
				continue
			}
			successDps = append(successDps, dpid)
		}
	}
	s.buildSuccessResp(w, fmt.Sprintf("restore partitions, success partitions: %v, failed partitions: %v, failed disks: %v", successDps, failedDps, failedDisks))
}

func (s *DataNode) getRaftStatus(w http.ResponseWriter, r *http.Request) {
	const (
		paramRaftID = "raftID"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	raftID, err := strconv.ParseUint(r.FormValue(paramRaftID), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramRaftID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	raftStatus := s.raftStore.RaftStatus(raftID)
	s.buildSuccessResp(w, raftStatus)
}

func (s *DataNode) getPartitionsAPI(w http.ResponseWriter, r *http.Request) {
	partitions := make([]interface{}, 0)
	var (
		riskCount        int
		riskFixerRunning bool
	)
	s.space.WalkPartitions(func(dp *DataPartition) bool {
		partition := &struct {
			ID                    uint64                  `json:"id"`
			Size                  int                     `json:"size"`
			Used                  int                     `json:"used"`
			Status                int                     `json:"status"`
			Path                  string                  `json:"path"`
			Replicas              []string                `json:"replicas"`
			NeedServerFaultCheck  bool                    `json:"needServerFaultCheck"`
			ServerFaultCheckLevel FaultOccurredCheckLevel `json:"serverFaultCheckLevel"`
			RiskFixerStatus       *riskdata.FixerStatus   `json:"riskFixerStatus"`
		}{
			ID:                    dp.ID(),
			Size:                  dp.Size(),
			Used:                  dp.Used(),
			Status:                dp.Status(),
			Path:                  dp.Path(),
			Replicas:              dp.getReplicaClone(),
			ServerFaultCheckLevel: dp.serverFaultCheckLevel,
			RiskFixerStatus: func() *riskdata.FixerStatus {
				if fixer := dp.RiskFixer(); fixer != nil {
					status := fixer.Status()
					riskCount += status.Count
					if status.Running {
						riskFixerRunning = true
					}
					return status
				}
				return nil
			}(),
		}
		partitions = append(partitions, partition)
		return true
	})
	result := &struct {
		Partitions       []interface{} `json:"partitions"`
		PartitionCount   int           `json:"partitionCount"`
		RiskCount        int           `json:"riskCount"`
		RiskFixerRunning bool          `json:"riskFixerRunning"`
	}{
		Partitions:       partitions,
		PartitionCount:   len(partitions),
		RiskCount:        riskCount,
		RiskFixerRunning: riskFixerRunning,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getExtentMd5Sum(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
		paramExtentID    = "extent"
		paramOffset      = "offset"
		paramSize        = "size"
	)
	var (
		err                                 error
		partitionID, extentID, offset, size uint64
		md5Sum                              string
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue(paramExtentID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if r.FormValue(paramOffset) != "" {
		if offset, err = strconv.ParseUint(r.FormValue(paramOffset), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramOffset, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if r.FormValue(paramSize) != "" {
		if size, err = strconv.ParseUint(r.FormValue(paramSize), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramSize, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("partition(%v) not exist", partitionID))
		return
	}
	if !partition.ExtentStore().IsExists(extentID) {
		s.buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("partition(%v) extentID(%v) not exist", partitionID, extentID))
		return
	}
	md5Sum, err = partition.ExtentStore().ComputeMd5Sum(extentID, offset, size)
	if err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, fmt.Sprintf("partition(%v) extentID(%v) computeMD5 failed %v", partitionID, extentID, err))
		return
	}
	result := &struct {
		PartitionID uint64 `json:"PartitionID"`
		ExtentID    uint64 `json:"ExtentID"`
		Md5Sum      string `json:"md5"`
	}{
		PartitionID: partitionID,
		ExtentID:    extentID,
		Md5Sum:      md5Sum,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getPartitionAPI(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
	)
	var (
		partitionID uint64
		files       []storage.ExtentInfoBlock
		err         error
		dpInfo      *DataPartitionViewInfo
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if dpInfo, err = partition.getDataPartitionInfo(); err != nil {
		s.buildFailureResp(w, http.StatusNotFound, "data partition info get failed")
		return
	}
	if files, err = partition.ExtentStore().GetAllWatermarks(proto.AllExtentType, nil); err != nil {
		err = fmt.Errorf("get watermark fail: %v", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	dpInfo.Files = files
	dpInfo.FileCount = len(files)
	s.buildSuccessResp(w, dpInfo)
}

func (s *DataNode) triggerPartitionError(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
		paramIsDiskError = "isDiskError"
		paramAuthCode    = "authCode"
	)
	var (
		err         error
		partitionID uint64
		authCode    string
		isDiskError bool
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse from fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	authCode = r.FormValue(paramAuthCode)
	if val := r.FormValue(paramIsDiskError); val != "" {
		var bVal bool
		if bVal, err = strconv.ParseBool(val); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse param %v fail: %v", paramIsDiskError, err))
			return
		}
		isDiskError = bVal
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if authCode != hex.EncodeToString(md5.New().Sum([]byte(partition.volumeID))) {
		s.buildFailureResp(w, http.StatusBadRequest, "authCode mismatch")
		return
	}

	var partitionErr error
	if isDiskError {
		partitionErr = syscall.EIO
	} else {
		partitionErr = storage.NewParameterMismatchErr("parameter mismatch")
	}
	partition.checkIsPartitionError(partitionErr)
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) getPartitionSimpleAPI(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
	)
	var (
		partitionID uint64
		err         error
		dpInfo      *DataPartitionViewInfo
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if dpInfo, err = partition.getDataPartitionInfo(); err != nil {
		s.buildFailureResp(w, http.StatusNotFound, "data partition info get failed")
		return
	}
	s.buildSuccessResp(w, dpInfo)
}

func (s *DataNode) getPartitionRaftHardStateAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	var partition = s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	var hs raftProto.HardState
	if hs, err = partition.RaftHardState(); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.buildSuccessResp(w, hs)
	return
}

func (s *DataNode) getExtentAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		extentInfo  *storage.ExtentInfoBlock
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().Watermark(uint64(extentID)); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, extentInfo)
	return
}

func (s *DataNode) getReplProtocalBufferDetail(w http.ResponseWriter, r *http.Request) {
	allReplDetail := repl.GetReplProtocolDetail()
	s.buildSuccessResp(w, allReplDetail)
	return
}

func (s *DataNode) getBlockCrcAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		blocks      []*storage.BlockCrc
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if proto.IsTinyExtent(uint64(extentID)) {
		s.buildFailureResp(w, http.StatusBadRequest, "can not query tiny extent")
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if blocks, err = partition.ExtentStore().ScanBlocks(uint64(extentID)); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, blocks)
	return
}

func (s *DataNode) buildSuccessResp(w http.ResponseWriter, data interface{}) {
	s.buildJSONResp(w, http.StatusOK, data, "")
}

func (s *DataNode) buildFailureResp(w http.ResponseWriter, code int, msg string) {
	s.buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func (s *DataNode) buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

func (s *DataNode) getStatInfo(w http.ResponseWriter, r *http.Request) {
	if s.processStatInfo == nil {
		s.buildFailureResp(w, http.StatusBadRequest, "data node is initializing")
		return
	}
	//get process stat info
	cpuUsageList, maxCPUUsage := s.processStatInfo.GetProcessCPUStatInfo()
	_, memoryUsedGBList, maxMemoryUsedGB, maxMemoryUsage := s.processStatInfo.GetProcessMemoryStatInfo()
	//get disk info
	disks := s.space.GetDisks()
	diskList := make([]interface{}, 0, len(disks))
	for _, disk := range disks {
		diskTotal, err := diskusage.GetDiskTotal(disk.Path)
		if err != nil {
			diskTotal = disk.Total
		}
		diskInfo := &struct {
			Path          string  `json:"path"`
			TotalTB       float64 `json:"totalTB"`
			UsedGB        float64 `json:"usedGB"`
			UsedRatio     float64 `json:"usedRatio"`
			ReservedSpace uint    `json:"reservedSpaceGB"`
		}{
			Path:          disk.Path,
			TotalTB:       unit.FixedPoint(float64(diskTotal)/unit.TB, 1),
			UsedGB:        unit.FixedPoint(float64(disk.Used)/unit.GB, 1),
			UsedRatio:     unit.FixedPoint(float64(disk.Used)/float64(diskTotal), 1),
			ReservedSpace: uint(disk.ReservedSpace / unit.GB),
		}
		diskList = append(diskList, diskInfo)
	}
	result := &struct {
		Type           string        `json:"type"`
		Zone           string        `json:"zone"`
		Version        interface{}   `json:"versionInfo"`
		StartTime      string        `json:"startTime"`
		CPUUsageList   []float64     `json:"cpuUsageList"`
		MaxCPUUsage    float64       `json:"maxCPUUsage"`
		CPUCoreNumber  int           `json:"cpuCoreNumber"`
		MemoryUsedList []float64     `json:"memoryUsedGBList"`
		MaxMemoryUsed  float64       `json:"maxMemoryUsedGB"`
		MaxMemoryUsage float64       `json:"maxMemoryUsage"`
		DiskInfo       []interface{} `json:"diskInfo"`
	}{
		Type:           ModuleName,
		Zone:           s.zoneName,
		Version:        proto.MakeVersion("DataNode"),
		StartTime:      s.processStatInfo.ProcessStartTime,
		CPUUsageList:   cpuUsageList,
		MaxCPUUsage:    maxCPUUsage,
		CPUCoreNumber:  cpu.GetCPUCoreNumber(),
		MemoryUsedList: memoryUsedGBList,
		MaxMemoryUsed:  maxMemoryUsedGB,
		MaxMemoryUsage: maxMemoryUsage,
		DiskInfo:       diskList,
	}
	s.buildSuccessResp(w, result)
}

func matchKey(key string) bool {
	return key == generateAuthKey()
}

func generateAuthKey() string {
	date := time.Now().Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func (s *DataNode) getTinyExtentHoleInfo(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	result, err := partition.getTinyExtentHoleInfo(uint64(extentID))
	if err != nil {
		s.buildFailureResp(w, http.StatusNotFound, err.Error())
		return
	}
	s.buildSuccessResp(w, result)
}

// 这个API用于回放指定Partition的TINYEXTENT_DELETE记录，为该Partition下的所有TINY EXTENT重新打洞
func (s *DataNode) playbackPartitionTinyDelete(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		count       uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	countStr := r.FormValue("count")
	if countStr != "" {
		if count, err = strconv.ParseUint(countStr, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	store := partition.ExtentStore()
	if err = store.PlaybackTinyDelete(int64(count * storage.DeleteTinyRecordSize)); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) stopPartitionById(partitionID uint64) (err error) {
	partition := s.space.Partition(partitionID)
	if partition == nil {
		err = fmt.Errorf("partition[%d] not exist", partitionID)
		return
	}
	partition.Disk().space.DetachDataPartition(partition.ID())
	partition.Disk().DetachDataPartition(partition)
	partition.Stop()
	return nil
}

func (s *DataNode) stopPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if err = s.stopPartitionById(partitionID); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	s.buildSuccessResp(w, nil)
}

func (s *DataNode) reloadPartitionByName(partitionPath, disk string) (err error) {
	var (
		partition   *DataPartition
		partitionID uint64
	)

	var diskPath, ok = ParseDiskPath(disk)
	if !ok {
		err = fmt.Errorf("illegal disk path: %v", disk)
		return
	}

	var d, exists = s.space.GetDisk(diskPath.Path())
	if !exists {
		err = fmt.Errorf("disk no exists: %v", disk)
		return
	}

	if partitionID, _, err = unmarshalPartitionName(partitionPath); err != nil {
		err = fmt.Errorf("action[reloadPartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
			partitionPath, disk, err.Error())
		return
	}

	partition = s.space.Partition(partitionID)
	if partition != nil {
		err = fmt.Errorf("partition[%d] exist, can not reload", partitionID)
		return
	}

	if err = s.space.LoadPartition(d, partitionID, partitionPath); err != nil {
		return
	}
	return
}

func (s *DataNode) reloadPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionPath string
		disk          string
		err           error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if disk = r.FormValue("disk"); disk == "" {
		s.buildFailureResp(w, http.StatusBadRequest, "param disk is empty")
		return
	}

	if partitionPath = r.FormValue("partitionPath"); partitionPath == "" {
		s.buildFailureResp(w, http.StatusBadRequest, "param partitionPath is empty")
		return
	}

	if err = s.reloadPartitionByName(partitionPath, disk); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	s.buildSuccessResp(w, nil)
}

func (s *DataNode) moveExtentFileToBackup(pathStr string, partitionID, extentID uint64) (err error) {
	const (
		repairDirStr = "repair_extents_backup"
	)
	rootPath := pathStr[:strings.LastIndex(pathStr, "/")]
	extentFilePath := path.Join(pathStr, strconv.Itoa(int(extentID)))
	now := time.Now()
	repairDirPath := path.Join(rootPath, repairDirStr, fmt.Sprintf("%d-%d-%d", now.Year(), now.Month(), now.Day()))
	os.MkdirAll(repairDirPath, 0655)
	fileInfo, err := os.Stat(repairDirPath)
	if err != nil || !fileInfo.IsDir() {
		err = fmt.Errorf("path[%s] is not exist or is not dir", repairDirPath)
		return
	}
	repairBackupFilePath := path.Join(repairDirPath, fmt.Sprintf("%d_%d_%d", partitionID, extentID, now.Unix()))
	log.LogWarnf("rename file[%s-->%s]", extentFilePath, repairBackupFilePath)
	if err = os.Rename(extentFilePath, repairBackupFilePath); err != nil {
		return
	}

	file, err := os.OpenFile(extentFilePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return
	}
	file.Close()
	return nil
}

func (s *DataNode) moveExtentFile(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err            error
		partitionID    uint64
		extentID       uint64
		pathStr        string
		partitionIDStr string
		extentIDStr    string
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("move extent %s/%s failed:%s", pathStr, extentIDStr, err.Error())
		}
	}()

	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	log.LogWarnf("move extent %s/%s begin", pathStr, extentIDStr)

	if len(partitionIDStr) == 0 || len(extentIDStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	if extentID, err = strconv.ParseUint(extentIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		return
	}

	if err = s.moveExtentFileToBackup(pathStr, partitionID, extentID); err != nil {
		return
	}

	log.LogWarnf("move extent %s/%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, nil)
	return
}

func (s *DataNode) moveExtentFileBatch(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err            error
		partitionID    uint64
		pathStr        string
		partitionIDStr string
		extentIDStr    string
		resultMap      map[uint64]string
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogWarnf("move extent batch partition:%s extents:%s failed:%s", pathStr, extentIDStr, err.Error())
		}
	}()

	resultMap = make(map[uint64]string)
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	extentIDArrayStr := strings.Split(extentIDStr, "-")
	log.LogWarnf("move extent batch partition:%s extents:%s begin", pathStr, extentIDStr)

	if len(partitionIDStr) == 0 || len(extentIDArrayStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	for _, idStr := range extentIDArrayStr {
		var ekId uint64
		if ekId, err = strconv.ParseUint(idStr, 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
			return
		}
		resultMap[ekId] = fmt.Sprintf("partition:%d extent:%d not start", partitionID, ekId)
	}

	for _, idStr := range extentIDArrayStr {
		ekId, _ := strconv.ParseUint(idStr, 10, 64)
		if err = s.moveExtentFileToBackup(pathStr, partitionID, ekId); err != nil {
			resultMap[ekId] = err.Error()
			log.LogErrorf("repair extent %s/%s failed", pathStr, idStr)
		} else {
			resultMap[ekId] = "OK"
			log.LogWarnf("repair extent %s/%s success", pathStr, idStr)
		}
	}
	err = nil
	log.LogWarnf("move extent batch partition:%s extents:%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, resultMap)
	return
}

func (s *DataNode) repairExtent(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err              error
		partitionID      uint64
		extentID         uint64
		hasStopPartition bool
		pathStr          string
		partitionIDStr   string
		extentIDStr      string
		lastSplitIndex   int
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("repair extent batch partition: %s extents[%s] failed:%s", pathStr, extentIDStr, err.Error())
		}

		if hasStopPartition {
			s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex])
		}

	}()

	hasStopPartition = false
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)

	if len(partitionIDStr) == 0 || len(extentIDStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	if extentID, err = strconv.ParseUint(extentIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		return
	}
	lastSplitIndex = strings.LastIndex(pathStr, "/")
	log.LogWarnf("repair extent %s/%s begin", pathStr, extentIDStr)

	if err = s.stopPartitionById(partitionID); err != nil {
		return
	}
	hasStopPartition = true

	if err = s.moveExtentFileToBackup(pathStr, partitionID, extentID); err != nil {
		return
	}

	hasStopPartition = false
	if err = s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex]); err != nil {
		return
	}
	log.LogWarnf("repair extent %s/%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, nil)
	return
}

func (s *DataNode) repairExtentBatch(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err              error
		partitionID      uint64
		hasStopPartition bool
		pathStr          string
		partitionIDStr   string
		extentIDStr      string
		resultMap        map[uint64]string
		lastSplitIndex   int
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("repair extent batch partition: %s extents[%s] failed:%s", pathStr, extentIDStr, err.Error())
		}

		if hasStopPartition {
			s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex])
		}
	}()

	resultMap = make(map[uint64]string)
	hasStopPartition = false
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	extentIDArrayStr := strings.Split(extentIDStr, "-")

	if len(partitionIDStr) == 0 || len(extentIDArrayStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	for _, idStr := range extentIDArrayStr {
		var ekId uint64
		if ekId, err = strconv.ParseUint(idStr, 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
			return
		}
		resultMap[ekId] = fmt.Sprintf("partition:%d extent:%d not start", partitionID, ekId)
	}
	lastSplitIndex = strings.LastIndex(pathStr, "/")
	log.LogWarnf("repair extent batch partition: %s extents[%s] begin", pathStr, extentIDStr)

	if err = s.stopPartitionById(partitionID); err != nil {
		return
	}
	hasStopPartition = true

	for _, idStr := range extentIDArrayStr {
		ekId, _ := strconv.ParseUint(idStr, 10, 64)
		if err = s.moveExtentFileToBackup(pathStr, partitionID, ekId); err != nil {
			resultMap[ekId] = err.Error()
			log.LogErrorf("repair extent %s/%s failed:%s", pathStr, idStr, err.Error())
		} else {
			resultMap[ekId] = "OK"
			log.LogWarnf("repair extent %s/%s success", pathStr, idStr)
		}
	}
	err = nil
	hasStopPartition = false
	if err = s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex]); err != nil {
		return
	}
	log.LogWarnf("repair extent batch partition: %s extents[%s] success", pathStr, extentIDStr)
	s.buildSuccessResp(w, resultMap)
	return
}

func (s *DataNode) getExtentCrc(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue("extentId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	log.LogDebugf("getExtentCrc partitionID(%v) extentID(%v)", partitionID, extentID)
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	store := partition.ExtentStore()
	crc, err := store.GetExtentCrc(extentID)
	if err != nil {
		log.LogErrorf("GetExtentCrc err(%v)", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := &struct {
		CRC uint32
	}{
		CRC: crc,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getFingerprint(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    uint64
		strict      bool
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue("extentId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	strict, _ = strconv.ParseBool(r.FormValue("strict"))
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	store := partition.ExtentStore()

	var eib *storage.ExtentInfoBlock
	if eib, err = store.Watermark(extentID); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	var fingerprint storage.Fingerprint
	if fingerprint, err = store.Fingerprint(extentID, 0, int64(eib[storage.Size]), strict); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := &struct {
		Fingerprint storage.Fingerprint
	}{
		Fingerprint: fingerprint,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) resetFaultOccurredCheckLevel(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID       uint64
		partitionStr      string
		level             uint64
		checkCorruptLevel FaultOccurredCheckLevel
		err               error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if level, err = strconv.ParseUint(r.FormValue("level"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	checkCorruptLevel, err = convertCheckCorruptLevel(level)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partitionStr = r.FormValue("partitionID")
	if partitionStr != "" {
		if partitionID, err = strconv.ParseUint(partitionStr, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		partition := s.space.Partition(partitionID)
		if partition == nil {
			s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
			return
		}
		partition.setFaultOccurredCheckLevel(checkCorruptLevel)
	} else {
		s.space.WalkPartitions(func(partition *DataPartition) bool {
			partition.setFaultOccurredCheckLevel(checkCorruptLevel)
			return true
		})
	}
	s.buildSuccessResp(w, "success")
}

func (s *DataNode) getSfxStatus(w http.ResponseWriter, r *http.Request) {
	disks := make([]interface{}, 0)
	for _, diskItem := range s.space.GetDisks() {
		disk := &struct {
			Path              string `json:"path"`
			IsSfx             bool   `json:"IsSfx"`
			DevName           string `json:"devName"`
			PhysicalUsedRatio uint32 `json:"PhysicalUsedRatio"`
			CompressionRatio  uint32 `json:"CompressionRatio"`
		}{
			Path:              diskItem.Path,
			IsSfx:             diskItem.IsSfx,
			DevName:           diskItem.devName,
			PhysicalUsedRatio: diskItem.PhysicalUsedRatio,
			CompressionRatio:  diskItem.CompressionRatio,
		}
		if disk.IsSfx {
			disks = append(disks, disk)
		}
	}
	diskReport := &struct {
		Disks []interface{} `json:"disks"`
		Zone  string        `json:"zone"`
	}{
		Disks: disks,
		Zone:  s.zoneName,
	}
	s.buildSuccessResp(w, diskReport)
}

func (s *DataNode) getExtentLockInfo(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID    uint64
		extentID       uint64
		extLockInfoMap = make(map[uint64]*proto.ExtentIdLockInfo)
		err            error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue("extentID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentID == 0 {
		partition.ExtentStore().RangeExtentLockInfo(func(key interface{}, value *ttlstore.Val) bool {
			eId := key.(uint64)
			extLockInfoMap[eId] = &proto.ExtentIdLockInfo{
				ExtentId:   eId,
				ExpireTime: value.GetExpirationTime(),
				TTL:        value.GetTTL(),
			}
			return true
		})
	} else {
		if value, ok := partition.ExtentStore().LoadExtentLockInfo(extentID); ok {
			extLockInfoMap[extentID] = &proto.ExtentIdLockInfo{
				ExtentId:   extentID,
				ExpireTime: value.GetExpirationTime(),
				TTL:        value.GetTTL(),
			}
		}
	}
	s.buildSuccessResp(w, extLockInfoMap)
	return
}

func (s *DataNode) getRiskStatus(w http.ResponseWriter, r *http.Request) {
	var result = &struct {
		Count   int  `json:"count"`
		Running bool `json:"running"`
	}{}
	s.space.WalkPartitions(func(partition *DataPartition) bool {
		if fixer := partition.RiskFixer(); fixer != nil {
			status := fixer.Status()
			result.Count += status.Count
			if status.Running {
				result.Running = true
			}
		}
		return true
	})
	s.buildSuccessResp(w, result)
}

func (s *DataNode) startRiskFix(w http.ResponseWriter, r *http.Request) {
	s.space.WalkPartitions(func(partition *DataPartition) bool {
		if fixer := partition.RiskFixer(); fixer != nil {
			fixer.Start()
		}
		return true
	})
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) stopRiskFix(w http.ResponseWriter, r *http.Request) {
	s.space.WalkPartitions(func(partition *DataPartition) bool {
		if fixer := partition.RiskFixer(); fixer != nil {
			fixer.Stop()
		}
		return true
	})
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) transferDeleteV0(w http.ResponseWriter, r *http.Request) {
	var (
		err          error
		partitionStr string
		partitionID  uint64
		failes       []uint64
		succeeds     []uint64
		archives     int
		lock         sync.Mutex
	)
	succeeds = make([]uint64, 0)
	failes = make([]uint64, 0)
	partitionStr = r.FormValue("partitionID")
	if partitionStr != "" {
		if partitionID, err = strconv.ParseUint(partitionStr, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		partition := s.space.Partition(partitionID)
		if partition == nil {
			s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
			return
		}
		archives, err = partition.extentStore.TransferDeleteV0()
		if err != nil {
			log.LogErrorf("transfer failed, partition: %v, err: %v", partitionID, err)
			s.buildFailureResp(w, http.StatusNotFound, err.Error())
			return
		}
		if archives > 0 {
			succeeds = append(succeeds, partitionID)
		}
	} else {
		s.transferDeleteLock.Lock()
		defer s.transferDeleteLock.Unlock()
		wg := sync.WaitGroup{}
		s.space.WalkDisks(func(disk *Disk) bool {
			wg.Add(1)
			go func() {
				defer wg.Done()
				disk.WalkPartitions(func(partition *DataPartition) bool {
					var archive int
					archive, err = partition.extentStore.TransferDeleteV0()
					if err != nil {
						lock.Lock()
						failes = append(failes, partition.partitionID)
						lock.Unlock()
						log.LogErrorf("transfer failed, partition: %v, err: %v", partitionID, err)
						return true
					}
					if archive == 0 {
						return true
					}
					lock.Lock()
					succeeds = append(succeeds, partition.partitionID)
					lock.Unlock()
					return true
				})
			}()
			return true
		})
		wg.Wait()
	}
	if len(succeeds) > 0 || len(failes) > 0 {
		s.buildSuccessResp(w, fmt.Sprintf("all partitions transfer finish, success: %v, failed: %v", succeeds, failes))
	} else {
		s.buildSuccessResp(w, "no partitions need to transfer")
	}
}

func (s *DataNode) getDataPartitionViewCache(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	volumeName := r.FormValue("volumeName")
	if volumeName == "" {
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("lack of params: volumeName"))
		return
	}
	dpIDStr := r.FormValue("dpID")
	if dpIDStr == "" {
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("lack of params: dpID"))
		return
	}
	dpID, err := strconv.ParseUint(dpIDStr, 10, 64)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	data := s.topoManager.GetPartitionFromCache(volumeName, dpID)
	s.buildSuccessResp(w, data)
	return
}

func (s *DataNode) buildHeartBeatResponsePb(response *proto.DataNodeHeartbeatResponsePb) {
	response.Status = proto.TaskSucceeds
	response.Version = DataNodeLatestVersion
	stat := s.space.Stats()
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Available = stat.Available
	response.CreatedPartitionCnt = uint32(stat.CreatedPartitionCnt)
	response.TotalPartitionSize = stat.TotalPartitionSize
	response.MaxCapacity = stat.MaxCapacityToCreatePartition
	response.RemainingCapacity = stat.RemainingCapacityToCreatePartition
	response.BadDisks = make([]string, 0)
	response.DiskInfos = make(map[string]*proto.DiskInfoPb, 0)
	stat.Unlock()

	response.HttpPort = s.httpPort
	response.ZoneName = s.zoneName
	response.PartitionReports = make([]*proto.PartitionReportPb, 0)
	space := s.space
	space.WalkPartitions(func(partition *DataPartition) bool {
		leaderAddr, isLeader := partition.IsRaftLeader()
		vr := &proto.PartitionReportPb{
			VolName:         partition.volumeID,
			PartitionID:     partition.ID(),
			PartitionStatus: int32(partition.Status()),
			Total:           uint64(partition.Size()),
			Used:            uint64(partition.Used()),
			DiskPath:        partition.Disk().Path,
			IsLeader:        isLeader,
			ExtentCount:     int32(partition.GetExtentCount()),
			NeedCompare:     true,
			IsLearner:       partition.IsRaftLearner(),
			IsRecover:       partition.DataPartitionCreateType == proto.DecommissionedCreateDataPartition,
		}
		log.LogDebugf("action[Heartbeats] dpid(%v), status(%v) total(%v) used(%v) leader(%v) isLeader(%v) isLearner(%v).",
			vr.PartitionID, vr.PartitionStatus, vr.Total, vr.Used, leaderAddr, vr.IsLeader, vr.IsLearner)
		response.PartitionReports = append(response.PartitionReports, vr)
		return true
	})

	disks := space.GetDisks()
	var usageRatio float64
	for _, d := range disks {
		usageRatio = 0
		if d.Total != 0 {
			usageRatio = float64(d.Used) / float64(d.Total)
		}
		dInfo := &proto.DiskInfoPb{Total: d.Total, Used: d.Used, ReservedSpace: d.ReservedSpace, Status: int32(d.Status), Path: d.Path, UsageRatio: float32(usageRatio), IsSFX: d.IsSfx}
		response.DiskInfos[d.Path] = dInfo
		if d.Status == proto.Unavailable {
			response.BadDisks = append(response.BadDisks, d.Path)
		}
	}
}

func (s *DataNode) buildHeartBeatResponse(response *proto.DataNodeHeartbeatResponse) {
	response.Status = proto.TaskSucceeds
	response.Version = DataNodeLatestVersion
	stat := s.space.Stats()
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Available = stat.Available
	response.CreatedPartitionCnt = uint32(stat.CreatedPartitionCnt)
	response.TotalPartitionSize = stat.TotalPartitionSize
	response.MaxCapacity = stat.MaxCapacityToCreatePartition
	response.RemainingCapacity = stat.RemainingCapacityToCreatePartition
	response.BadDisks = make([]string, 0)
	response.DiskInfos = make(map[string]*proto.DiskInfo, 0)
	stat.Unlock()

	response.HttpPort = s.httpPort
	response.ZoneName = s.zoneName
	response.PartitionReports = make([]*proto.PartitionReport, 0)
	space := s.space
	space.WalkPartitions(func(partition *DataPartition) bool {
		leaderAddr, isLeader := partition.IsRaftLeader()
		vr := &proto.PartitionReport{
			VolName:         partition.volumeID,
			PartitionID:     partition.ID(),
			PartitionStatus: partition.Status(),
			Total:           uint64(partition.Size()),
			Used:            uint64(partition.Used()),
			DiskPath:        partition.Disk().Path,
			IsLeader:        isLeader,
			ExtentCount:     partition.GetExtentCount(),
			NeedCompare:     true,
			IsLearner:       partition.IsRaftLearner(),
			IsRecover:       partition.DataPartitionCreateType == proto.DecommissionedCreateDataPartition,
			IsSFX:           partition.disk.IsSfx,
		}
		log.LogDebugf("action[Heartbeats] dpid(%v), status(%v) total(%v) used(%v) leader(%v) isLeader(%v) isLearner(%v).",
			vr.PartitionID, vr.PartitionStatus, vr.Total, vr.Used, leaderAddr, vr.IsLeader, vr.IsLearner)
		response.PartitionReports = append(response.PartitionReports, vr)
		return true
	})

	disks := space.GetDisks()
	var usageRatio float64
	for _, d := range disks {
		usageRatio = 0
		if d.Total != 0 {
			usageRatio = float64(d.Used) / float64(d.Total)
		}
		dInfo := &proto.DiskInfo{Total: d.Total, Used: d.Used, ReservedSpace: d.ReservedSpace, Status: d.Status, Path: d.Path, UsageRatio: usageRatio, IsSFX: d.IsSfx}
		response.DiskInfos[d.Path] = dInfo
		if d.Status == proto.Unavailable {
			response.BadDisks = append(response.BadDisks, d.Path)
		}
	}
}
