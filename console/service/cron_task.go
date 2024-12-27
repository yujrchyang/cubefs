package service

import (
	"github.com/cubefs/cubefs/console/model"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"time"

	"github.com/cubefs/cubefs/console/service/scheduleTask"
	"github.com/cubefs/cubefs/console/service/traffic"
	"github.com/cubefs/cubefs/util/log"
	"github.com/robfig/cron"
)

func InitCronApiSdk() {
	table := model.ConsoleCluster{}
	clusters, err := table.LoadConsoleClusterList("")
	if err != nil {
		log.LogErrorf("initCronApiSdk: load console cluster from database failed: err: %v", err)
		return
	}

	sdk := api.NewAPIManager(clusters)
	api.SetSdkApiManager(sdk)
	log.LogInfof("CronApiSdk init success")
}

func InitCronTask(stopC chan bool) {
	InitCronApiSdk()
	log.LogInfof("InitCronTask: time(%v)", time.Now())
	c := cron.New()
	// 每天15点40统计vol请求数
	c.AddFunc("40 15 * * *", traffic.CollectVolumeOps)
	// 每小时15分,迁移volInfo历史数据
	c.AddFunc("15 0/1 * * *", traffic.MigrateVolumeHistoryData)
	// 每10倍数分钟触发一次(容量、使用量、inode数、可写dp数、client_count) 保留3天
	c.AddFunc("0/10 * * * *", traffic.CollectVolumeInfo)
	// 每小时1次 获取冷热迁移vol的hdd/ssd介质占比
	c.AddFunc("* 0/1 * * *", scheduleTask.CollectVolHddSsdCapacity)
	// 每小时1次(整点)，降低并发，或请求节点而非master
	// c.AddFunc("0 * * * *", traffic.CollectHostUsedInfo)

	c.Start()
	defer c.Stop()

	select {
	case <-stopC:
		log.LogInfof("exit TrafficCronTask: now(%v)", time.Now().Format(time.DateTime))
		return
	}
}
