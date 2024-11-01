package checktool

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool/dongdong"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
)

const (
	DefaultMinCount     = 3
	DefaultWarnInternal = 5 * 60
)

var ddAlarmMap = new(sync.Map)

func WarnBySpecialUmpKey(umpKey, msg string) {
	log.LogWarn(msg)
	exporter.WarningBySpecialUMPKey(umpKey, msg)
}

// WarnByDongDongAlarmToTargetGid
//
//	buff.WriteString(fmt.Sprintf("【警告】%v\n", msg))
//	buff.WriteString(fmt.Sprintf("【应用】%v\n", app))
//	buff.WriteString(fmt.Sprintf("【采集点】%v\n", umpKey))
func WarnByDongDongAlarmToTargetGid(targetGid int, app, umpKey, msg string) {
	log.LogWarnf(msg)
	defer HandleCrash()
	var (
		err           error
		targetDDAlarm *dongdong.CommonAlarm
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[warnToTargetGidByDongDongAlarm] err:%v", err)
		}
	}()
	if load, ok := ddAlarmMap.Load(targetGid); ok {
		targetDDAlarm = load.(*dongdong.CommonAlarm)
	}
	if targetDDAlarm == nil {
		if targetDDAlarm, err = dongdong.NewCommonAlarm(targetGid, app); err != nil {
			return
		}
		ddAlarmMap.Store(targetGid, targetDDAlarm)
	}
	err = targetDDAlarm.Alarm(umpKey, msg)
	return
}

func HandleCrash() {
	if r := recover(); r != nil {
		debug.PrintStack()
		flushPanicLog(r)
	}
}

func flushPanicLog(r interface{}) {
	callers := ""
	for i := 0; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		callers = callers + fmt.Sprintf("%v:%v\n", file, line)
	}
	log.LogErrorf("Recovered from panic: %#v (%v)\n%v", r, r, callers)
	log.LogFlush()
}

func ReDirPath(path string) string {
	_, err := os.Stat(path)
	if err == nil {
		return path
	}
	dir, err1 := filepath.Abs(filepath.Dir(os.Args[0]))
	if err1 != nil {
		return path
	}
	return filepath.Join(dir, path)
}

func Md5(rawStr string) (sign string) {
	h := md5.New()
	h.Write([]byte(rawStr))
	cipherStr := h.Sum(nil)
	sign = hex.EncodeToString(cipherStr)
	return
}

func RemoveDuplicateElement(stringList []string) []string {
	result := make([]string, 0, len(stringList))
	temp := map[string]struct{}{}
	for _, item := range stringList {
		if _, ok := temp[item]; !ok {
			temp[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}
