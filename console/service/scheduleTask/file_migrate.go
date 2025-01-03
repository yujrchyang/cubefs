package scheduleTask

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
)

type FileMigrateWorker struct {
	api *api.APIManager // todo:数据库配worker地址
}

func NewFileMigrateWorker(api *api.APIManager) *FileMigrateWorker {
	worker := new(FileMigrateWorker)
	worker.api = api
	return worker
}

func (fm *FileMigrateWorker) GetMigrateConfigs(cluster, prefix string) ([]*cproto.MigrateConfigView, error) {
	configList, err := fm.api.MigrateConfigList(cluster)
	if err != nil {
		return nil, err
	}
	result := make([]*cproto.MigrateConfigView, 0, len(configList))
	for _, config := range configList {
		if strings.HasPrefix(config.VolName, prefix) {
			view := cproto.FormatMigrateConfigView(config)
			result = append(result, view)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].UpdateAt > result[j].UpdateAt
	})
	return result, nil
}

func (fm *FileMigrateWorker) GetVolHddSsdDataHistory(cluster, volume string, start, end time.Time) ([]*model.VolumeMigrateConfig, error) {
	records, err := model.LoadVolHddSsdCapacityData(cluster, volume, start, end)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		record.UpdateAt = record.UpdateTime.Format(time.DateTime)
	}
	return records, nil
}

func (fm *FileMigrateWorker) CreateMigrateConfig(cluster, volume string, smart, migrateBack, compact int, hddDirs string,
	rulesType cproto.RulesType, timeUnit cproto.RulesUnit, timeValue int64) error {
	// 已有迁移规则，直接覆盖
	//oldConfig, _ := fm.api.GetVolumeMigrateConfig(cluster, volume)
	//if oldConfig != nil {
	//	return fmt.Errorf("该vol(%v)已存在迁移规则", volume)
	//}
	params := make(map[string]string)
	params["smart"] = strconv.Itoa(smart)
	params["migrationBack"] = strconv.Itoa(migrateBack)
	params["compact"] = strconv.Itoa(compact)
	params["hddDirs"] = hddDirs
	smartRule, err := cproto.ParseSmartRules(rulesType, timeUnit, timeValue)
	if err != nil {
		return err
	}
	params["smartRules"] = smartRule
	err = fm.api.CreateOrUpdateMigrateConfig(cluster, volume, params)
	return err
}

func (fm *FileMigrateWorker) UpdateMigrateConfig(cluster, volume string, smart, migrateBack, compact int, hddDirs string,
	rulesType cproto.RulesType, timeUnit cproto.RulesUnit, timeValue int64) error {

	oldConfig, err := fm.api.GetVolumeMigrateConfig(cluster, volume)
	if err != nil {
		return err
	}
	params := make(map[string]string)
	if oldConfig.Smart != smart {
		params["smart"] = strconv.Itoa(smart)
	}
	if oldConfig.MigrationBack != migrateBack {
		params["migrationBack"] = strconv.Itoa(migrateBack)
	}
	if oldConfig.Compact != compact {
		params["compact"] = strconv.Itoa(compact)
	}
	if oldConfig.HddDirs != hddDirs {
		params["hddDirs"] = hddDirs
	}
	smartRule, err := cproto.ParseSmartRules(rulesType, timeUnit, timeValue)
	if err != nil {
		return err
	}
	params["smartRules"] = smartRule
	err = fm.api.CreateOrUpdateMigrateConfig(cluster, volume, params)
	return err
}

// 批量开启/关闭 smart状态
func (fm *FileMigrateWorker) BatchUpdateSmart(cluster string, vols []string, smart int) error {
	var errResult error
	params := make(map[string]string)
	params["smart"] = strconv.Itoa(smart)
	for _, vol := range vols {
		err := fm.api.CreateOrUpdateMigrateConfig(cluster, vol, params)
		if err != nil {
			errResult = fmt.Errorf("%vvol(%v) err(%v)", errResult, vol, err)
		}
	}
	if errResult != nil {
		log.LogErrorf("BatchUpdateSmart failed: cluster(%v) smart(%v) err(%v)", cluster, smart, errResult)
	}
	return errResult
}

var (
	bracketReg = regexp.MustCompile(`\((.*)\)`)
)

func FindRegStr(dir string) (reg []string) {
	matches := bracketReg.FindAllStringSubmatch(dir, -1)
	if len(matches) == 1 {
		return matches[0]
	}
	return
}

func CollectVolHddSsdCapacity() {
	sdk := api.GetSdkApiManager()

	clusters := make([]*model.ConsoleCluster, 0)
	clusterInfos := sdk.GetConsoleCluster()
	for _, clusterInfo := range clusterInfos {
		if clusterInfo.FileMigrateHost != "" {
			clusters = append(clusters, clusterInfo)
		}
	}
	wg := new(sync.WaitGroup)
	volChan := make(chan *model.VolumeMigrateConfig, 128)
	for _, cluster := range clusters {
		volConfigs, err := sdk.MigrateConfigList(cluster.ClusterName)
		if err != nil {
			log.LogErrorf("CollectVolHddSsdCapacity: get Migrate vol failed: cluster(%v) err(%v)", cluster.ClusterName, err)
			return
		}
		// 并发获取vol的信息
		wg.Add(1)
		go getVolHddSsdCapacity(wg, volChan, volConfigs, sdk)
	}
	go func() {
		wg.Wait()
		close(volChan)
	}()

	records := make([]*model.VolumeMigrateConfig, 0)
	for volConfig := range volChan {
		records = append(records, volConfig)
	}
	_ = model.BatchInsertVolConfig(records)
}

func getVolHddSsdCapacity(clusterWg *sync.WaitGroup, recordChan chan<- *model.VolumeMigrateConfig, volMigConfig []*cproto.MigrateConfig, sdk *api.APIManager) {
	defer func() {
		clusterWg.Done()

		if time.Now().Minute() == 0 {
			// 整点，清理数据
			log.LogInfof("CleanExpiredVolMigrateConfig: time(%v)", time.Now())
			for _, vol := range volMigConfig {
				model.CleanExpiredVolMigrateConfig(vol.ClusterName, vol.VolName)
			}
		}
	}()
	// 只有spark集群
	var mc *master.MasterClient
	if len(volMigConfig) > 0 {
		cluster := volMigConfig[0].ClusterName
		mc = sdk.GetMasterClient(cluster)
	}

	wg := new(sync.WaitGroup)
	c := make(chan struct{}, 5)
	for _, migConfig := range volMigConfig {
		if migConfig.Smart < 1 {
			continue
		}
		wg.Add(1)
		c <- struct{}{}
		go func(config *cproto.MigrateConfig) {
			defer func() {
				wg.Done()
				<-c
			}()
			dataPartitions, err := mc.ClientAPI().GetDataPartitions(config.VolName, nil)
			if err != nil {
				log.LogErrorf("getVolHddSsdCapacity: get partitions failed, err(%v)", err)
				return
			}
			var (
				hddUsed    uint64
				ssdUsed    uint64
				replicaNum int
			)
			for _, partition := range dataPartitions.DataPartitions {
				switch partition.MediumType {
				case "ssd":
					ssdUsed += partition.Used
				case "hdd":
					hddUsed += partition.Used
				}
				if int(partition.ReplicaNum) > replicaNum {
					replicaNum = int(partition.ReplicaNum)
				}
			}
			record := &model.VolumeMigrateConfig{
				ClusterName:   config.ClusterName,
				VolName:       config.VolName,
				Smart:         config.Smart,
				SmartRules:    config.SmartRules,
				SsdDirs:       config.SsdDirs,
				HddDirs:       config.HddDirs,
				Compact:       config.Compact,
				MigrationBack: config.MigrationBack,
				ReplicaNum:    replicaNum,
				HddCapacity:   math.Trunc(float64(hddUsed/1024/1024/1024*1000)) / 1000,
				SsdCapacity:   math.Trunc(float64(ssdUsed/1024/1024/1024*1000)) / 1000,
				UpdateTime:    time.Now(),
			}
			recordChan <- record
		}(migConfig)
	}
	wg.Wait()
}
