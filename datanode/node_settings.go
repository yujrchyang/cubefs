package datanode

import (
	"encoding/json"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"os"
	"path"
	"sync"
	"time"
)

type NodeSettings struct {
	SwitchMap      map[string]bool   `json:"switchMap"`
	TimeoutMap     map[string]uint64 `json:"timeoutMap,omitempty"`
	ThresholdMap   map[string]uint64 `json:"thresholdMap,omitempty"`
	UpdateTime     string            `json:"updateTime"`
	path           string
	onSwitchChange func(switchName string, stat bool) error
	sync.RWMutex
}

func NewNodeSettings(path string, onSwitchChange func(string, bool) error) *NodeSettings {
	return &NodeSettings{
		path:           path,
		SwitchMap:      make(map[string]bool),
		TimeoutMap:     make(map[string]uint64),
		ThresholdMap:   make(map[string]uint64),
		onSwitchChange: onSwitchChange,
	}
}

// parse
// 1 switches: disableBlacklist,...
// 2 timeouts:
// 3 thresholds:
// ...
func (ns *NodeSettings) parse() (err error) {
	ns.RLock()
	defer ns.RUnlock()
	settingFileName := path.Join(ns.path, DataSettingsFile)
	_, err = os.Stat(settingFileName)
	if err != nil {
		if os.IsNotExist(err) {
			log.LogWarnf("action[parse] %v not exist, using default node setting", settingFileName)
			err = nil
		}
		return
	}
	var c *config.Config
	c, err = config.LoadConfigFile(settingFileName)
	if err != nil {
		return
	}

	// load switches
	switches := make(map[string]bool)
	bufSwitch := c.GetJsonObjectBytes("switchMap")
	if len(bufSwitch) > 0 {
		if err = json.Unmarshal(bufSwitch, &switches); err != nil {
			return err
		}
		ns.SwitchMap = switches
	}

	for m, s := range ns.SwitchMap {
		err = ns.onSwitchChange(m, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ns *NodeSettings) updateAndPersistSwitch(switchName string, newStat bool) (err error) {
	ns.Lock()
	defer ns.Unlock()
	oldStat := ns.SwitchMap[switchName]
	if oldStat == newStat {
		return
	}
	log.LogWarnf("action[updateAndPersistSwitch] set switch(%v) from (%v) to (%v)", switchName, oldStat, newStat)
	ns.SwitchMap[switchName] = newStat
	if err = ns.onSwitchChange(switchName, newStat); err != nil {
		ns.SwitchMap[switchName] = oldStat
		return err
	}

	if err = ns.persist(); err != nil {
		ns.SwitchMap[switchName] = oldStat
		_ = ns.onSwitchChange(switchName, oldStat)
		log.LogErrorf("action[updateAndPersistSwitch] set switch(%v) err:%v, rollback to old stat:%v", switchName, err, oldStat)
	}
	return err
}

func (ns *NodeSettings) persist() (err error) {
	var newData []byte
	ns.UpdateTime = time.Now().Format(TimeLayout)
	if newData, err = json.Marshal(ns); err != nil {
		return
	}
	tmpSettingFileName := path.Join(ns.path, TempDataSettingsFile)
	settingFileName := path.Join(ns.path, DataSettingsFile)

	var tmp *os.File
	if tmp, err = os.OpenFile(tmpSettingFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0666); err != nil {
		return
	}
	defer func() {
		_ = tmp.Close()
		if err != nil {
			_ = os.Remove(tmpSettingFileName)
		}
	}()
	if _, err = tmp.Write(newData); err != nil {
		return
	}
	if err = tmp.Sync(); err != nil {
		return
	}
	if err = os.Rename(tmpSettingFileName, settingFileName); err != nil {
		return
	}
	return err
}
