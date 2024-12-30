package service

import (
	"fmt"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/console/service/scheduleTask"
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
