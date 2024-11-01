package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"strings"
)

const hbaseOwner = "cloudnativecsi"
const defaultAlarmWater = 0.85

var gAlaramCapWaterLevel float64

func (s *ChubaoFSMonitor) checkSparkHbaseCap() {
	gAlaramCapWaterLevel = defaultAlarmWater
	for _, host := range s.hosts {
		if host.host != DomainSpark {
			continue
		}
		checkHbaseCap(host)
	}
}

func checkHbaseCap(clusterHost *ClusterHost) {
	client := master.NewMasterClient([]string{clusterHost.host}, false)
	vols, err := client.AdminAPI().ListVols("")
	if err != nil {
		log.LogErrorf("list vol err:%s", err.Error())
		return
	}

	for _, vol := range vols {
		if strings.Contains(vol.Owner, hbaseOwner) && len(vol.Owner) == len(hbaseOwner) {
			if vol.UsedRatio >= gAlaramCapWaterLevel {
				log.LogErrorf("vol:%s cap beyond %0.3f,now:%0.3f, owner:%s\n", vol.Name, gAlaramCapWaterLevel, vol.UsedRatio, vol.Owner)
				warnBySpecialUmpKeyWithPrefix(UmpHBaseCapBeyondLevelKey, fmt.Sprintf("vol:%s cap beyond %0.3f,now:%0.3f, owner:%s\n", vol.Name, gAlaramCapWaterLevel, vol.UsedRatio, vol.Owner))
			}
		}
	}
	return
}
