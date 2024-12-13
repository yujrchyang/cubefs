package cfs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/multi_email"
	"github.com/cubefs/cubefs/util/checktool/ump"
	"github.com/cubefs/cubefs/util/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var prefix = "a"

const (
	domainSeparator       = ","
	dbBakClusterSeparator = "#"

	TB                                      = 1024 * 1024 * 1024 * 1024
	GB                                      = 1024 * 1024 * 1024
	defaultMpNoLeaderWarnInternal           = 10 * 60
	defaultMpNoLeaderMinCount               = 3
	keyWordReleaseCluster                   = "seqwrite"
	defaultMaxInactiveNodes                 = 2
	defaultMaxOfflineDataNodes              = 3
	defaultMaxOfflineMetaNodes              = 3
	defaultMaxOfflineDisks                  = 10
	minOfflineDiskThreshold                 = time.Minute * 5
	defaultMNDiskMinWarnSize                = GB * 20
	defaultMNDiskMinWarnRatio               = 0.7
	defaultMetaNodeUsedRatioMinThresholdSSD = 0.75
	defaultDataNodeUsedRatioMinThresholdSSD = 0.85
	defaultMetaNodeUsedRatioMinThresholdHDD = 0.75
	defaultDataNodeUsedRatioMinThresholdHDD = 0.85
	checkPeerConcurrency                    = 8
	defaultNodeRapidMemIncWarnThreshold     = 20 //内存使用率(%)
	defaultNodeRapidMemIncreaseWarnRatio    = 0.05
	minMetaNodeExportDiskUsedRatio          = 70
	defaultRestartNodeMaxCountIn24Hour      = 3
	maxBadDataPartitionsCount               = 200
	maxBadDataPartitionsCountMysql          = 150
	minWarnFaultToUsersCheckInterval        = 60 * 5
	defaultMaxPendQueueCount                = 0
	defaultMaxAppliedIDDiffCount            = 100
	defaultMaxOfflineFlashNodesIn24Hour     = 5
	defaultExpiredMetaRemainDays            = 1
)

// config key
const (
	cfgKeyUsedRatio                        = "usedRatio"
	cfgKeyAvailSpaceRatio                  = "availSpaceRatio"
	cfgKeyReadWriteDpRatio                 = "readWriteDpRatio"
	cfgKeyClusterUsedRatio                 = "clusterUsedRatio"
	cfgKeyCheckFlashNode                   = "checkFlashNode"
	cfgKeyCheckRiskFix                     = "checkRiskFix"
	cfgKeyFlashNodeValidVersions           = "flashNodeVersions"
	cfgKeyCheckAvailTinyVols               = "checkAvailTinyVols"
	cfgKeyNlClusterUsedRatio               = "nlClusterUsedRatio"
	cfgKeyMinRWCnt                         = "minRWCnt"
	cfgKeyInterval                         = "interval"
	cfgKeyDpCheckInterval                  = "dpCheckInterval"
	cfgKeyMpCheckInterval                  = "mpCheckInterval"
	cfgKeyMaxOfflineDataNodes              = "maxOfflineDataNodes"
	cfgKeyMaxOfflineDisks                  = "maxOfflineDisks"
	cfgKeyMinOfflineDiskMinute             = "minOfflineDiskMinute"
	cfgKeyMetaNodeUsedRatioMinThresholdSSD = "metaNodeUsedRatioMinThresholdSSD"
	cfgKeyDataNodeUsedRatioMinThresholdSSD = "dataNodeUsedRatioMinThresholdSSD"
	cfgKeyMetaNodeUsedRatioMinThresholdHDD = "metaNodeUsedRatioMinThresholdHDD"
	cfgKeyDataNodeUsedRatioMinThresholdHDD = "dataNodeUsedRatioMinThresholdHDD"
	cfgKeyCheckPeerConcurrency             = "checkPeerConcurrency"
	cfsKeymasterJsonPath                   = "cfsmasterJsonPath"
	cfgMinRWDPAndMPVolsJsonPath            = "minRWDPAndMPVolsJsonPath"
	cfsKeyWarnFaultToUsersJsonPath         = "cfsWarnFaultToUsersJsonPath"
	cfgKeyClusterConfigCheckJsonPath       = "clusterConfigCheckJsonPath"
	cfgKeyDPMaxPendQueueCount              = "dpMaxPendQueueCount"
	cfgKeyDPMaxAppliedIDDiffCount          = "dpMaxAppliedIDDiffCount"
	cfgKeyMPMaxPendQueueCount              = "mpMaxPendQueueCount"
	cfgKeyMPMaxAppliedIDDiffCount          = "mpMaxAppliedIDDiffCount"
	cfgKeyDPPendQueueAlarmThreshold        = "dpPendQueueAlarmThreshold"
	cfgKeyMPPendQueueAlarmThreshold        = "mpPendQueueAlarmThreshold"
	cfgKeyMetaNodeExportDiskUsedRatio      = "metaNodeExportDiskUsedRatio"
	cfgKeyIgnoreCheckMP                    = "ignoreCheckMP"
	cfgKeyNodeRapidMemIncWarnThreshold     = "nodeRapidMemIncWarnThreshold"
	cfgKeyNodeRapidMemIncreaseWarnRatio    = "nodeRapidMemIncreaseWarnRatio"
	cfgKeyExpiredMetaRemainDays            = "expiredMetaRemainDays"
	cfgKeyHDDDiskOfflineInterval           = "hddDiskOfflineInterval"
	cfgKeySSDDiskOfflineInterval           = "ssdDiskOfflineInterval"
	cfgKeyHDDDiskOfflineThreshold          = "hddDiskOfflineThreshold"
	cfgKeySSDDiskOfflineThreshold          = "ssdDiskOfflineThreshold"
	cfgKeyDisableCleanExpiredMP            = "disableCleanExpiredMP"
	cfgKeyEnable                           = "enable"
	cfgKeyEnvironment                      = "env" //online or test
	cfgKeyConfigsPath                      = "configsPath"
)

var configKeys []string

var intKeys = []string{
	cfgKeyHDDDiskOfflineInterval,
	cfgKeySSDDiskOfflineInterval,
	cfgKeyHDDDiskOfflineThreshold,
	cfgKeySSDDiskOfflineThreshold,
}

const (
	errorConnRefused = "connection refused"
)

const (
	dataNodeType  = 0
	metaNodeType  = 1
	flashNodeType = 2
)

const (
	DevelopmentEnv = "development"
	ProductionEnv  = "production"
)

var env string

func initEnv(e string) {
	env = e
}

func isProEnv() bool {
	return env == ProductionEnv
}

func isDevEnv() bool {
	return env == DevelopmentEnv
}

var (
	checkVolWg         sync.WaitGroup
	checkNodeWg        sync.WaitGroup
	noLeaderMps        *sync.Map
	checkMasterNodesWg sync.WaitGroup
	masterNodesMutex   sync.Mutex
	checkDpCorruptWg   sync.WaitGroup
	checkMpCorruptWg   sync.WaitGroup
)

var (
	bucketName    string
	s3Client      *s3.Client
)

type ChubaoFSMonitor struct {
	envConfig                               *EnvConfig
	usedRatio                               float64
	availSpaceRatio                         float64
	readWriteDpRatio                        float64
	hosts                                   []*ClusterHost
	minReadWriteCount                       int64
	scheduleInterval                        int
	clusterUsedRatio                        float64
	nlClusterUsedRatio                      float64
	metrics                                 map[string]*AlarmMetric
	chubaoFSMasterNodes                     map[string][]string
	badDiskXBPTickets                       *sync.Map            //map[string]XBPTicketInfo
	markDeleteVols                          map[string]time.Time // host#volName:lastWarnTime
	offlineDiskMaxCountIn24Hour             int
	offlineDiskThreshold                    time.Duration
	masterLbLastWarnInfo                    map[string]*MasterLBWarnInfo
	scheduleDpCheckInterval                 int
	scheduleMpCheckInterval                 int
	MinRWDPAndMPVols                        []MinRWDPAndMPVolInfo
	lastZoneDataNodeDiskUsedRatioAlarmTime  time.Time
	lastZoneDataNodeDiskUsedRatioTelAlarm   time.Time
	lastZoneDataNodeDiskUsedRatioTelOpAlarm time.Time
	lastZoneMetaNodeDiskUsedRatioAlarmTime  time.Time
	lastZoneMetaNodeDiskUsedRatioTelAlarm   time.Time
	lastZoneMetaNodeDiskUsedRatioTelOpAlarm time.Time
	lastRestartNodeTime                     time.Time
	lastCheckStartTime                      map[string]time.Time
	RestartNodeMaxCountIn24Hour             int
	highLoadNodeSolver                      *ChubaoFSHighLoadNodeSolver
	volNeedAllocateDPContinuedTimes         map[string]int
	WarnFaultToUsers                        []*WarnFaultToTargetUsers
	WarnFaultToUsersCheckInterval           int
	sreDB                                   *gorm.DB
	metaNodeExportDiskUsedRatio             float64
	ignoreCheckMp                           bool
	nodeRapidMemIncWarnThreshold            float64
	nodeRapidMemIncreaseWarnRatio           float64
	metaNodeUsedRatioMinThresholdHDD        float64
	dataNodeUsedRatioMinThresholdHDD        float64
	metaNodeUsedRatioMinThresholdSSD        float64
	dataNodeUsedRatioMinThresholdSSD        float64
	checkPeerConcurrency                    int
	checkFlashNode                          bool
	flashNodeValidVersions                  []string
	umpClient                               *ump.UMPClient
	clusterConfigCheck                      *ClusterConfigCheck
	ExpiredMetaRemainDaysCfg                int
	checkAvailTinyVols                      []string
	disableCleanExpiredMP                   bool
	fixBadPartition                         bool
	ctx                                     context.Context
	dpReleaser                              *ChubaoFSDPReleaser
	checkRiskFix                            bool
	configMap                               map[string]string
	integerMap                              map[string]int64
	pushJMQInterval                         int64
}

func NewChubaoFSMonitor(ctx context.Context) *ChubaoFSMonitor {
	return &ChubaoFSMonitor{
		metrics:                         make(map[string]*AlarmMetric, 0),
		chubaoFSMasterNodes:             make(map[string][]string),
		badDiskXBPTickets:               new(sync.Map),
		markDeleteVols:                  make(map[string]time.Time),
		masterLbLastWarnInfo:            make(map[string]*MasterLBWarnInfo),
		volNeedAllocateDPContinuedTimes: make(map[string]int),
		WarnFaultToUsers:                make([]*WarnFaultToTargetUsers, 0),
		lastCheckStartTime:              make(map[string]time.Time),
		clusterConfigCheck:              new(ClusterConfigCheck),
		configMap:                       make(map[string]string, 0),
		integerMap:                      make(map[string]int64, 0),
		ctx:                             ctx,
	}
}

func (s *ChubaoFSMonitor) Start(cfg *config.Config) (err error) {
	err = s.parseConfig(cfg)
	if err != nil {
		return
	}
	noLeaderMps = new(sync.Map)
	s.umpClient = ump.NewUmpClient(cfg.GetString(cfgUmpAPiToken), umpOpenAPiDomain)
	s.scheduleTask(cfg)
	releaser := startChubaoFSDPReleaser(cfg)
	if releaser == nil {
		err = fmt.Errorf("init dp releaser failed")
		return
	}
	s.dpReleaser = releaser
	highLoadNodeSolver := StartChubaoFSHighLoadNodeSolver(s.sreDB)
	if highLoadNodeSolver != nil {
		s.RestartNodeMaxCountIn24Hour = defaultRestartNodeMaxCountIn24Hour
		s.highLoadNodeSolver = highLoadNodeSolver
	}
	s.registerHandler()
	fmt.Println("starting ChubaoFSMonitor finished")
	return
}

func (s *ChubaoFSMonitor) extractChubaoFSInfo(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	if err = json.Unmarshal(cfg.Raw, &s.chubaoFSMasterNodes); err != nil {
		return
	}
	log.LogInfof("chubaoFSMasterNodes: %v", s.chubaoFSMasterNodes)
	return
}

func (s *ChubaoFSMonitor) extractMinRWDPAndMPVols(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	volInfos := struct {
		MinRWDPAndMPVols []MinRWDPAndMPVolInfo
	}{}
	if err = json.Unmarshal(cfg.Raw, &volInfos); err != nil {
		return
	}
	s.MinRWDPAndMPVols = volInfos.MinRWDPAndMPVols
	for _, vol := range s.MinRWDPAndMPVols {
		if s.envConfig.Env == DevelopmentEnv && isOnlineDomain(vol.Host) {
			panic("can not using online domain for MinRWDPAndMPVols in dev environment")
		}
	}
	fmt.Println("MinRWDPAndMPVols:", s.MinRWDPAndMPVols)
	return
}

func (s *ChubaoFSMonitor) extractWarnFaultToUsers(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	userInfos := struct {
		WarnFaultToUsersCheckInterval int
		WarnFaultToUsers              []*WarnFaultToTargetUsers
	}{}
	if err = json.Unmarshal(cfg.Raw, &userInfos); err != nil {
		return
	}
	s.WarnFaultToUsers = userInfos.WarnFaultToUsers
	s.WarnFaultToUsersCheckInterval = userInfos.WarnFaultToUsersCheckInterval
	if s.WarnFaultToUsersCheckInterval < minWarnFaultToUsersCheckInterval {
		s.WarnFaultToUsersCheckInterval = minWarnFaultToUsersCheckInterval
	}
	fmt.Println("WarnFaultToUsersCheckInterval:", s.WarnFaultToUsersCheckInterval)
	if marshal, err1 := json.Marshal(s.WarnFaultToUsers); err1 == nil {
		fmt.Println("WarnFaultToUsers:", string(marshal))
	}
	return
}

func (s *ChubaoFSMonitor) extractClusterConfigCheck(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	if err = json.Unmarshal(cfg.Raw, s.clusterConfigCheck); err != nil {
		return
	}
	fmt.Println("clusterConfigCheck:", s.clusterConfigCheck)
	return
}

func (s *ChubaoFSMonitor) scheduleTask(cfg *config.Config) {
	go s.scheduleToCheckVol()
	go s.scheduleToCheckSpecificVol()
	go s.scheduleToCheckNodesAlive()
	go s.scheduleToCheckClusterUsedRatio()
	go s.scheduleToCheckMasterNodesAlive()
	go s.scheduleToCheckObjectNodeAlive()
	go s.scheduleToCheckMpPeerCorrupt()
	go s.scheduleToCheckDpPeerCorrupt()
	if isProEnv() {
		log.LogInfof("start schedulers for production environment")
		go s.scheduleToCheckAndWarnFaultToUsers()
		go s.NewSchedule(s.fixOnlineBadDataPartition, time.Minute)
		go s.scheduleToCheckZoneDiskUsedRatio()
		go s.scheduleToCheckXBPTicket()
		go s.scheduleToCheckMasterLbPodStatus()
		go s.scheduleToCheckMetaPartitionSplit()
		go s.scheduleToCheckClusterConfig()
		go s.scheduleToCheckZoneMnDnWriteAbilityRate()
		go s.NewSchedule(s.checkMasterMetadata, time.Hour)
		go s.NewSchedule(s.checkSparkHbaseCap, time.Minute*30)
		go s.NewSchedule(s.checkOnlineCoreVols, time.Minute*2)
		go s.scheduleToReloadDP()
	} else {
		log.LogInfof("disable some schedulers for development environment")
	}
	go s.scheduleToCheckCFSHighIncreaseMemNodes()
	go s.NewSchedule(s.checkDiskError, time.Minute*1)
	go s.NewScheduleV2(s.checkUnavailableDataPartition, time.Minute*15)
	go s.NewSchedule(s.checkDataNodeRiskData, time.Hour)
	go s.NewSchedule(s.checkNodeSet, time.Hour)
	go s.NewSchedule(s.resetTokenMap, time.Minute*30)
	go s.NewSchedule(s.checkDbbakDataPartition, time.Hour*6)
	go s.NewSchedule(s.checkAvailableTinyExtents, time.Minute*2)
	go s.NewSchedule(s.CheckMetaPartitionApply, time.Minute*30)
	go s.NewSchedule(s.clientAlarm, clientAlarmInterval)
}

func (s *ChubaoFSMonitor) scheduleToCheckVol() {
	s.checkAvailSpaceAndVolsStatus()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkAvailSpaceAndVolsStatus()
		}
	}
}

func isOnlineDomain(domain string) bool {
	switch domain {
	case DomainSpark, DomainMysql, DomainDbbak, DomainNL, DomainOchama:
		return true
	default:
		return false
	}
}

func (s *ChubaoFSMonitor) parseConfig(cfg *config.Config) (err error) {
	var envConfig *EnvConfig
	configsPath := cfg.GetString(cfgKeyConfigsPath)
	envConfig, err = s.parseEnv(configsPath, cfg)
	if err != nil {
		return
	}
	s.envConfig = envConfig
	if err = s.initSreDBConfig(); err != nil {
		return
	}
	cfsMasterJsonPath := cfg.GetString(cfsKeymasterJsonPath)
	if cfsMasterJsonPath == "" {
		return fmt.Errorf("cfsMasterJsonPath is empty")
	}
	if err = s.extractChubaoFSInfo(cfsMasterJsonPath); err != nil {
		return fmt.Errorf("parse cfsmasterJsonPath failed, cfsmasterJsonPath can not be nil err:%v", err)
	}
	if err = s.parseChubaoFSDomains(); err != nil {
		return
	}
	if s.envConfig.Env == DevelopmentEnv {
		for _, h := range s.hosts {
			if isOnlineDomain(h.host) {
				panic("can not using online cfsDomains in dev environment")
			}
		}
	}
	if err = s.parseBucket(); err != nil {
		return err
	}
	minRWDPAndMPVolsJson := cfg.GetString(cfgMinRWDPAndMPVolsJsonPath)
	if minRWDPAndMPVolsJson == "" {
		return fmt.Errorf("cfgMinRWDPAndMPVolsJsonPath is empty")
	}
	if err = s.extractMinRWDPAndMPVols(minRWDPAndMPVolsJson); err != nil {
		return fmt.Errorf("parse cfgMinRWDPAndMPVolsJsonPath failed, cfsmasterJsonPath can not be nil err:%v", err)
	}
	clusterConfigCheckJsonPath := cfg.GetString(cfgKeyClusterConfigCheckJsonPath)
	if clusterConfigCheckJsonPath == "" {
		return fmt.Errorf("clusterConfigCheckJsonPath is empty")
	}
	if err = s.extractClusterConfigCheck(clusterConfigCheckJsonPath); err != nil {
		return fmt.Errorf("parse clusterConfigCheckJsonPath failed, clusterConfigCheckJsonPath can not be nil err:%v", err)
	}
	useRatio := cfg.GetFloat(cfgKeyUsedRatio)
	if useRatio <= 0 {
		return fmt.Errorf("parse usedRatio failed")
	}
	s.usedRatio = useRatio
	availSpaceRatio := cfg.GetFloat(cfgKeyAvailSpaceRatio)
	if availSpaceRatio <= 0 {
		return fmt.Errorf("parse availSpaceRatio failed")
	}
	s.availSpaceRatio = availSpaceRatio
	readWriteDpRatio := cfg.GetFloat(cfgKeyReadWriteDpRatio)
	if readWriteDpRatio <= 0 {
		return fmt.Errorf("parse availSpaceRatio failed")
	}
	s.readWriteDpRatio = readWriteDpRatio
	minRWCnt := cfg.GetFloat(cfgKeyMinRWCnt)
	if minRWCnt <= 0 {
		return fmt.Errorf("parse minRWCnt failed")
	}
	s.minReadWriteCount = int64(minRWCnt)

	s.updateMaxPendQueueAndMaxAppliedIDDiffCountByConfig(cfg)
	interval := cfg.GetString(cfgKeyInterval)
	if interval == "" {
		return fmt.Errorf("parse interval failed,interval can not be nil")
	}

	if s.scheduleInterval, err = strconv.Atoi(interval); err != nil {
		return err
	}
	// dp corrupt check
	dpCheckInterval := cfg.GetString(cfgKeyDpCheckInterval)
	if dpCheckInterval == "" {
		return fmt.Errorf("parse dpCheckInterval failed,dpCheckInterval can not be nil")
	}
	if s.scheduleDpCheckInterval, err = strconv.Atoi(dpCheckInterval); err != nil {
		return err
	}
	// mp corrupt check
	mpCheckInterval := cfg.GetString(cfgKeyMpCheckInterval)
	if mpCheckInterval == "" {
		return fmt.Errorf("parse mpCheckInterval failed,mpCheckInterval can not be nil")
	}
	if s.scheduleMpCheckInterval, err = strconv.Atoi(mpCheckInterval); err != nil {
		return err
	}

	offlineDataNodeMaxCount, _ := strconv.Atoi(cfg.GetString(cfgKeyMaxOfflineDataNodes))
	if offlineDataNodeMaxCount <= 0 {
		offlineDataNodeMaxCount = 1
	}
	if offlineDataNodeMaxCount > defaultMaxOfflineDataNodes {
		offlineDataNodeMaxCount = defaultMaxOfflineDataNodes
	}
	for _, host := range s.hosts {
		host.offlineDataNodeTokenPool = newTokenPool(time.Hour*24, offlineDataNodeMaxCount)
	}

	s.offlineDiskMaxCountIn24Hour, _ = strconv.Atoi(cfg.GetString(cfgKeyMaxOfflineDisks))
	if s.offlineDiskMaxCountIn24Hour <= 0 {
		s.offlineDiskMaxCountIn24Hour = 1
	}
	if s.offlineDiskMaxCountIn24Hour > defaultMaxOfflineDisks {
		s.offlineDiskMaxCountIn24Hour = defaultMaxOfflineDisks
	}
	s.clusterUsedRatio = cfg.GetFloat(cfgKeyClusterUsedRatio)
	if s.clusterUsedRatio <= 0 {
		return fmt.Errorf("parse clusterUsedRatio failed")
	}
	s.nlClusterUsedRatio = cfg.GetFloat(cfgKeyNlClusterUsedRatio)
	if s.nlClusterUsedRatio <= 0 {
		return fmt.Errorf("parse nlClusterUsedRatio failed")
	}
	offlineDiskMinMinute, _ := strconv.Atoi(cfg.GetString(cfgKeyMinOfflineDiskMinute))
	s.offlineDiskThreshold = time.Minute * time.Duration(offlineDiskMinMinute)
	if s.offlineDiskThreshold < minOfflineDiskThreshold {
		s.offlineDiskThreshold = minOfflineDiskThreshold
	}
	s.checkFlashNode = cfg.GetBool(cfgKeyCheckFlashNode)
	s.checkRiskFix = cfg.GetBool(cfgKeyCheckRiskFix)
	s.flashNodeValidVersions = cfg.GetStringSlice(cfgKeyFlashNodeValidVersions)
	if cfsWarnFaultToUsersJsonPath := cfg.GetString(cfsKeyWarnFaultToUsersJsonPath); cfsWarnFaultToUsersJsonPath != "" {
		if err = s.extractWarnFaultToUsers(cfsWarnFaultToUsersJsonPath); err != nil {
			return fmt.Errorf("parse cfsWarnFaultToUsersJsonPath failed,detail:%v err:%v", cfsWarnFaultToUsersJsonPath, err)
		}
	}
	s.parseHighMemNodeWarnConfig(cfg)
	s.metaNodeExportDiskUsedRatio = cfg.GetFloat(cfgKeyMetaNodeExportDiskUsedRatio)
	if s.metaNodeExportDiskUsedRatio <= 0 {
		fmt.Printf("parse %v failed use default value\n", cfgKeyMetaNodeExportDiskUsedRatio)
	}
	if s.metaNodeExportDiskUsedRatio < minMetaNodeExportDiskUsedRatio {
		s.metaNodeExportDiskUsedRatio = minMetaNodeExportDiskUsedRatio
	}
	s.ignoreCheckMp = cfg.GetBool(cfgKeyIgnoreCheckMP)
	s.metaNodeUsedRatioMinThresholdSSD = cfg.GetFloat(cfgKeyMetaNodeUsedRatioMinThresholdSSD)
	if s.metaNodeUsedRatioMinThresholdSSD <= 0 {
		fmt.Printf("parse %v failed use default value\n", cfgKeyMetaNodeUsedRatioMinThresholdSSD)
		s.metaNodeUsedRatioMinThresholdSSD = defaultMetaNodeUsedRatioMinThresholdSSD
	}
	s.dataNodeUsedRatioMinThresholdSSD = cfg.GetFloat(cfgKeyDataNodeUsedRatioMinThresholdSSD)
	if s.dataNodeUsedRatioMinThresholdSSD <= 0 {
		fmt.Printf("parse %v failed use default value\n", cfgKeyDataNodeUsedRatioMinThresholdSSD)
		s.dataNodeUsedRatioMinThresholdSSD = defaultDataNodeUsedRatioMinThresholdSSD
	}
	s.metaNodeUsedRatioMinThresholdHDD = cfg.GetFloat(cfgKeyMetaNodeUsedRatioMinThresholdHDD)
	if s.metaNodeUsedRatioMinThresholdHDD <= 0 {
		fmt.Printf("parse %v failed use default value\n", cfgKeyMetaNodeUsedRatioMinThresholdHDD)
		s.metaNodeUsedRatioMinThresholdHDD = defaultMetaNodeUsedRatioMinThresholdHDD
	}
	s.dataNodeUsedRatioMinThresholdHDD = cfg.GetFloat(cfgKeyDataNodeUsedRatioMinThresholdHDD)
	if s.dataNodeUsedRatioMinThresholdHDD <= 0 {
		fmt.Printf("parse %v failed use default value\n", cfgKeyDataNodeUsedRatioMinThresholdHDD)
		s.dataNodeUsedRatioMinThresholdHDD = defaultDataNodeUsedRatioMinThresholdHDD
	}

	s.checkPeerConcurrency = int(cfg.GetInt(cfgKeyCheckPeerConcurrency))
	if s.checkPeerConcurrency <= 0 || s.checkPeerConcurrency > 20 {
		fmt.Printf("parse %v failed use default value\n", cfgKeyCheckPeerConcurrency)
		s.checkPeerConcurrency = checkPeerConcurrency
	}
	s.ExpiredMetaRemainDaysCfg, _ = strconv.Atoi(cfg.GetString(cfgKeyExpiredMetaRemainDays))
	if s.ExpiredMetaRemainDaysCfg <= 0 {
		s.ExpiredMetaRemainDaysCfg = defaultExpiredMetaRemainDays
	}
	s.checkAvailTinyVols = cfg.GetStringSlice(cfgKeyCheckAvailTinyVols)

	if err = loadDockerIPList(); err != nil {
		return
	}
	for _, k := range configKeys {
		val := cfg.GetString(k)
		if val == "" {
			return fmt.Errorf("config key: %v can not be nil", k)
		}
		s.configMap[k] = cfg.GetString(k)
	}
	s.disableCleanExpiredMP = cfg.GetBool(cfgKeyDisableCleanExpiredMP)
	s.fixBadPartition = cfg.GetBool(cfgFixBadPartition)
	for _, k := range intKeys {
		s.integerMap[k] = cfg.GetInt64(k)
	}
	s.pushJMQInterval = int64(cfg.GetFloat(cfgPushJMQInterval))

	log.LogInfof("usedRatio[%v],availSpaceRatio[%v],readWriteDpRatio[%v],minRWCnt[%v],domains[%v],scheduleInterval[%v],clusterUsedRatio[%v]"+
		",offlineDiskMaxCountIn24Hour[%v],offlineDiskThreshold[%v],  mpCheckInterval[%v], "+
		"dpCheckInterval[%v],metaNodeExportDiskUsedRatio[%v],ignoreCheckMp[%v],metaNodeUsedRatioMinThresholdSSD[%v],dataNodeUsedRatioMinThresholdSSD[%v],"+
		"metaNodeUsedRatioMinThresholdHDD[%v],dataNodeUsedRatioMinThresholdHDD[%v],pushJMQInterval[%v]\n",
		s.usedRatio, s.availSpaceRatio, s.readWriteDpRatio, s.minReadWriteCount, s.hosts, s.scheduleInterval, s.clusterUsedRatio,
		s.offlineDiskMaxCountIn24Hour, s.offlineDiskThreshold, s.scheduleMpCheckInterval, s.scheduleDpCheckInterval,
		s.metaNodeExportDiskUsedRatio, s.ignoreCheckMp, s.metaNodeUsedRatioMinThresholdSSD, s.dataNodeUsedRatioMinThresholdSSD, s.metaNodeUsedRatioMinThresholdHDD, s.dataNodeUsedRatioMinThresholdHDD,
		s.pushJMQInterval)
	return
}

func (s *ChubaoFSMonitor) parseBucket() (err error) {
	if s.envConfig.Bucket == nil {
		log.LogWarnf("no bucket config specified")
		return
	}
	httpClient := &http.Client{
		Timeout: 2 * time.Minute,
	}
	bucketName = s.envConfig.Bucket.BucketName
	log.LogInfof("bucket config: %v", s.envConfig.Bucket)
	s3Client = s3.NewFromConfig(aws.Config{
		HTTPClient: httpClient,
	},
		func(o *s3.Options) {
			o.BaseEndpoint = aws.String("http://" + s.envConfig.Bucket.EndPoint)
		},
		func(o *s3.Options) {
			o.Credentials = credentials.NewStaticCredentialsProvider(s.envConfig.Bucket.AccessKey, s.envConfig.Bucket.SecretKey, "")
		},
		func(o *s3.Options) {
			o.Region = s.envConfig.Bucket.Region
		},
	)
	return
}

type EnvConfig struct {
	Env             string            `json:"env"`
	ObjectAppName   string            `json:"objectAppName"`
	CfsDomains      string            `json:"cfsDomains"`
	SreDbConfig     string            `json:"sreDbConfig"`
	Email           *EmailConfig      `json:"email"`
	JcloudOssDomain string            `json:"jcloudOssDomain"`
	Xbp             *XbpConfig        `json:"xbp"`
	Jdos            *JDOSConfig       `json:"jdos"`
	Ump             *UmpConfig        `json:"ump"`
	GroupAlarm      *GroupAlarmConfig `json:"groupAlarm"`
	Bucket          *BucketConfig     `json:"bucket"`
	JMQ             *JMQConfig        `json:"jmq"`
}

type XbpConfig struct {
	UserName     string `json:"userName"`
	Domain       string `json:"domain"`
	Sign         string `json:"sign"`
	ApiUser      string `json:"apiUser"`
	OfflineXbpID int    `json:"offlineXbpID"`
}

type JDOSConfig struct {
	JdosToken   string `json:"jdosToken"`
	JdosURL     string `json:"jdosURL"`
	JdosErp     string `json:"jdosErp"`
	JdosSysName string `json:"jdosSysName"`
}

type UmpConfig struct {
	UmpKeyStorageBotPrefix string `json:"umpKeyStorageBotPrefix"`
	UmpToken               string `json:"umpToken"`
}

// GroupAlarmConfig 咚咚消息通知
type GroupAlarmConfig struct {
	AlarmGid     int    `json:"alarmGid"`
	AlarmAppName string `json:"alarmAppName"`
}

type BucketConfig struct {
	BucketName string `json:"bucketName"`
	EndPoint   string `json:"endPoint"`
	Region     string `json:"region"`
	AccessKey  string `json:"ak"`
	SecretKey  string `json:"sk"`
}

type EmailConfig struct {
	Enable   bool           `json:"enable"`
	Property *EmailProperty `json:"property"`
}

type EmailProperty struct {
	SmtpHost string   `json:"smtpHost"`
	SmtpPort int      `json:"smtpPort"`
	MailFrom string   `json:"mailFrom"`
	MailTo   []string `json:"mailTo"`
	MailUser string   `json:"mailUser"`
	MailPwd  string   `json:"mailPwd"`
}

func (s *ChubaoFSMonitor) parseEnv(configsPath string, cfg *config.Config) (envConfig *EnvConfig, err error) {
	envStr := cfg.GetString(cfgKeyEnvironment)
	var envJsonFile string
	switch envStr {
	case DevelopmentEnv:
		initEnv(DevelopmentEnv)
		envJsonFile = path.Join(configsPath, "env_dev.json")
	case ProductionEnv:
		initEnv(ProductionEnv)
		envJsonFile = path.Join(configsPath, "env_pro.json")
	default:
		return nil, fmt.Errorf("unknown env: %v", envStr)
	}
	var wd string
	wd, err = os.Getwd()
	var c *config.Config
	c, err = config.LoadConfigFile(path.Join(wd, envJsonFile))
	if err != nil {
		return
	}

	envConfig = new(EnvConfig)
	if err = json.Unmarshal(c.Raw, envConfig); err != nil {
		return
	}
	if envConfig.Env != envStr {
		return nil, fmt.Errorf("invalid environment %v %v", envConfig.Env, envStr)
	}
	if envConfig.Ump.UmpKeyStorageBotPrefix == "" {
		return nil, fmt.Errorf("UmpKeyStorageBotPrefix is empty")
	}
	if envConfig.GroupAlarm.AlarmAppName == "" {
		return nil, fmt.Errorf("AlarmAppName is empty")
	}
	if envConfig.GroupAlarm.AlarmGid == 0 {
		return nil, fmt.Errorf("AlarmGid is empty")
	}
	if envConfig.Jdos.JdosSysName == "" {
		return nil, fmt.Errorf("empty jdos system name")
	}
	ResetUmpKeyPrefix(envConfig.Ump.UmpKeyStorageBotPrefix)
	dongDongAlarmApp = envConfig.GroupAlarm.AlarmAppName
	dongDongAlarmGid = envConfig.GroupAlarm.AlarmGid
	// objectAppName用来检查object node存活，依赖 JDOS Open Api，如果为空则跳过检查
	ResetObjectNodeAppName(envConfig.ObjectAppName)
	if envConfig.Ump.UmpToken == "" {
		err = fmt.Errorf("ump token not found in config")
		return
	}
	email := envConfig.Email
	if email.Enable {
		multi_email.InitMultiMail(email.Property.SmtpPort, email.Property.SmtpHost, email.Property.MailFrom, email.Property.MailUser, email.Property.MailPwd, email.Property.MailTo)
	}
	log.LogInfof("env config: email:%v xbp:%v ump:%v group:%v mysql:%v bucket:%v jdos:%v jmq:%v",
		envConfig.Email, envConfig.Xbp, envConfig.Ump, envConfig.GroupAlarm, envConfig.SreDbConfig, envConfig.Bucket, envConfig.Jdos, envConfig.JMQ)
	return
}

func (s *ChubaoFSMonitor) parseChubaoFSDomains() (err error) {
	if s.envConfig.CfsDomains == "" {
		err = fmt.Errorf("parse cfsDomains failed,cfsDomains can not be nil")
		return
	}
	hosts := strings.Split(s.envConfig.CfsDomains, domainSeparator)
	clusterHosts := make([]*ClusterHost, 0)
	for _, hostStr := range hosts {
		host := strings.Split(hostStr, dbBakClusterSeparator)[0]
		isRelease := strings.Split(hostStr, dbBakClusterSeparator)[1] == "1"
		clusterHosts = append(clusterHosts, newClusterHost(host, isRelease))
	}
	s.hosts = clusterHosts
	for _, host := range s.hosts {
		if masterNodes, ok := s.chubaoFSMasterNodes[host.host]; ok {
			host.masterNodes = masterNodes
			log.LogInfof("domain: %v chubaoFSMasterNodes: %v\n", host.host, s.chubaoFSMasterNodes)
		}
	}
	return
}
func (s *ChubaoFSMonitor) updateMaxPendQueueAndMaxAppliedIDDiffCountByConfig(cfg *config.Config) {
	dpMaxPendQueueCount, _ := strconv.Atoi(cfg.GetString(cfgKeyDPMaxPendQueueCount))
	dpMaxAppliedIDDiffCount, _ := strconv.Atoi(cfg.GetString(cfgKeyDPMaxAppliedIDDiffCount))
	mpMaxPendQueueCount, _ := strconv.Atoi(cfg.GetString(cfgKeyMPMaxPendQueueCount))
	mpMaxAppliedIDDiffCount, _ := strconv.Atoi(cfg.GetString(cfgKeyMPMaxAppliedIDDiffCount))

	dpPendQueueAlarmThreshold, _ := strconv.Atoi(cfg.GetString(cfgKeyDPPendQueueAlarmThreshold))
	mpPendQueueAlarmThreshold, _ := strconv.Atoi(cfg.GetString(cfgKeyMPPendQueueAlarmThreshold))
	if dpMaxPendQueueCount < defaultMaxPendQueueCount {
		dpMaxPendQueueCount = defaultMaxPendQueueCount
	}
	if mpMaxPendQueueCount < defaultMaxPendQueueCount {
		mpMaxPendQueueCount = defaultMaxPendQueueCount
	}
	if dpMaxAppliedIDDiffCount < defaultMaxAppliedIDDiffCount {
		dpMaxAppliedIDDiffCount = defaultMaxAppliedIDDiffCount
	}
	if mpMaxAppliedIDDiffCount < defaultMaxAppliedIDDiffCount {
		mpMaxAppliedIDDiffCount = defaultMaxAppliedIDDiffCount
	}
	if dpPendQueueAlarmThreshold < 2 {
		dpPendQueueAlarmThreshold = 2
	}
	if mpPendQueueAlarmThreshold < 2 {
		mpPendQueueAlarmThreshold = 2
	}
	for _, clusterHost := range s.hosts {
		clusterHost.DPMaxPendQueueCount = dpMaxPendQueueCount
		clusterHost.DPMaxAppliedIDDiffCount = dpMaxAppliedIDDiffCount
		clusterHost.MPMaxPendQueueCount = mpMaxPendQueueCount
		clusterHost.MPMaxAppliedIDDiffCount = mpMaxAppliedIDDiffCount
		clusterHost.DPPendQueueAlarmThreshold = dpPendQueueAlarmThreshold
		clusterHost.MPPendQueueAlarmThreshold = mpPendQueueAlarmThreshold
	}
	fmt.Printf("hosts:%v dpMaxPendQueueCount:%v dpMaxAppliedIDDiffCount:%v mpMaxPendQueueCount:%v mpMaxAppliedIDDiffCount:%v dpPendQueueAlarmThreshold:%v mpPendQueueAlarmThreshold:%v\n",
		s.hosts, dpMaxPendQueueCount, dpMaxAppliedIDDiffCount, mpMaxPendQueueCount, mpMaxAppliedIDDiffCount, dpPendQueueAlarmThreshold, mpPendQueueAlarmThreshold)
}

func (s *ChubaoFSMonitor) initSreDBConfig() (err error) {
	if s.envConfig.SreDbConfig == "" {
		fmt.Println("sre DBConfigDSN is empty")
		return
	}
	fmt.Println("cfgKeySreDbConfigDSNPort:", s.envConfig.SreDbConfig)
	s.sreDB, err = gorm.Open(mysql.New(mysql.Config{
		DSN:                       s.envConfig.SreDbConfig,
		DefaultStringSize:         256,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据版本自动配置
	}), &gorm.Config{})
	if err != nil {
		fmt.Println("init sreDB failed err:", err)
		s.sreDB = nil
		return
	}
	return
}

func (s *ChubaoFSMonitor) parseHighMemNodeWarnConfig(cfg *config.Config) {
	s.nodeRapidMemIncWarnThreshold = cfg.GetFloat(cfgKeyNodeRapidMemIncWarnThreshold)
	if s.nodeRapidMemIncWarnThreshold <= 0 {
		s.nodeRapidMemIncWarnThreshold = defaultNodeRapidMemIncWarnThreshold
		fmt.Printf("parse %v failed use default value\n", cfgKeyNodeRapidMemIncreaseWarnRatio)
	}
	s.nodeRapidMemIncreaseWarnRatio = cfg.GetFloat(cfgKeyNodeRapidMemIncreaseWarnRatio)
	if s.nodeRapidMemIncreaseWarnRatio <= 0 {
		s.nodeRapidMemIncreaseWarnRatio = defaultNodeRapidMemIncreaseWarnRatio
		fmt.Printf("parse %v failed use default value\n", cfgKeyNodeRapidMemIncWarnThreshold)
	}
	fmt.Printf("parseHighMemNodeWarnConfig nodeRapidMemIncWarnThreshold:%v ,nodeRapidMemIncreaseWarnRatio:%v\n", s.nodeRapidMemIncWarnThreshold, s.nodeRapidMemIncreaseWarnRatio)
	return
}
