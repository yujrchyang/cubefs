package checktool

import (
	"fmt"
	"testing"
	"time"
)

func TestDongDongAlarm(t *testing.T) {
	gid := 10212363418
	app := "test-storage-bot"
	go func() {
		for i := 0; i < 10; i++ {
			WarnByDongDongAlarmToTargetGid(gid, app, "test.storage.dongdong.1.1", fmt.Sprintf("this is unit test message for gid-1-1-%v", i))
		}
	}()
	go func() {
		for i := 0; i < 10; i++ {
			WarnByDongDongAlarmToTargetGid(gid, app, "test.storage.dongdong.1.2", fmt.Sprintf("this is unit test message for gid-1-2-%v", i))
		}
	}()
	time.Sleep(time.Second * 3)
}
