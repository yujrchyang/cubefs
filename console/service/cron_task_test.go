package service

import (
	"fmt"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/console/service/scheduleTask"
	"github.com/cubefs/cubefs/console/service/traffic"
	"testing"
)

func Test_CollectVolHddSsdCapacity(t *testing.T) {
	InitCronApiSdk()
	if api.GetSdkApiManager() == nil {
		t.Fatalf("init cron sdk failed: sdk is nil")
	}
	scheduleTask.CollectVolHddSsdCapacity()
	fmt.Println("finish collect!")
}

func Test_CollectHostUsedInfo(t *testing.T) {
	InitCronApiSdk()
	if api.GetSdkApiManager() == nil {
		t.Fatalf("init cron sdk failed: sdk is nil")
	}
	traffic.CollectHostUsedInfo()
	fmt.Println("finish collect!")
}
