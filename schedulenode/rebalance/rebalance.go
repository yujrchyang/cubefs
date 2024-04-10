package rebalance

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/util/log"
	"path"
	"time"
)

func (rw *ReBalanceWorker) ReBalanceStart(clusterHost, zoneName string, rType RebalanceType, highRatio, lowRatio, goalRatio float64,
	maxBatchCount int, migrateLimitPerDisk, dstMetaNodePartitionMaxCount int) error {
	ctrl, err := rw.newZoneCtrl(clusterHost, zoneName, rType, maxBatchCount, highRatio, lowRatio, goalRatio, migrateLimitPerDisk,
		dstMetaNodePartitionMaxCount)
	if err != nil {
		return err
	}
	if maxBatchCount > 0 {
		ctrl.SetClusterMaxBatchCount(maxBatchCount)
	}
	ctrl.SetMigrateLimitPerDisk(migrateLimitPerDisk)
	err = ctrl.ReBalanceStart()
	return err
}

func (rw *ReBalanceWorker) ReSetControlParam(clusterHost, zoneName string, rType RebalanceType, goalRatio float64,
	maxBatchCount, migrateLimitPerDisk, dstMNPartitionMaxCount int) (err error) {
	var (
		ctrl *ZoneReBalanceController
		rInfo *RebalancedInfoTable
	)
	if ctrl, err = rw.getZoneCtrl(clusterHost, zoneName, rType); err != nil {
		return
	}
	if rInfo, err = rw.GetRebalancedInfoByHostAndZoneName(clusterHost, zoneName, rType); err != nil {
		return
	}
	if goalRatio <= 0 {
		goalRatio = rInfo.GoalRatio
	}
	if err = ctrl.UpdateRatio(rInfo.HighRatio, rInfo.LowRatio, goalRatio); err != nil {
		return
	}
	if maxBatchCount <= 0 {
		maxBatchCount = rInfo.MaxBatchCount
	}
	if migrateLimitPerDisk <= 0 {
		migrateLimitPerDisk = rInfo.MigrateLimitPerDisk
	}
	if dstMNPartitionMaxCount <= 0 || dstMNPartitionMaxCount > defaultDstMetaNodePartitionMaxCount {
		dstMNPartitionMaxCount = defaultDstMetaNodePartitionMaxCount
	}
	if rInfo, err = rw.insertOrUpdateRebalancedInfo(clusterHost, zoneName, rType, maxBatchCount,
		rInfo.HighRatio, rInfo.LowRatio, goalRatio, migrateLimitPerDisk, dstMNPartitionMaxCount, Status(rInfo.Status)); err != nil {
		return
	}
	ctrl.dstMetaNodeMaxPartitionCount = dstMNPartitionMaxCount
	ctrl.SetClusterMaxBatchCount(maxBatchCount)
	ctrl.SetMigrateLimitPerDisk(migrateLimitPerDisk)
	return
}

func (rw *ReBalanceWorker) ReBalanceStop(cluster, zoneName string, rType RebalanceType) error {
	ctrl, err := rw.getZoneCtrl(cluster, zoneName, rType)
	if err != nil {
		return err
	}
	err = ctrl.ReBalanceStop()
	if err != nil {
		return err
	}
	return nil
}

func (rw *ReBalanceWorker) ReBalanceStatus(cluster, zoneName string, rType RebalanceType) (Status, error) {
	ctrl, err := rw.getZoneCtrl(cluster, zoneName, rType)
	if err != nil {
		return -1, err
	}
	status := ctrl.Status()
	return status, nil
}

func (rw *ReBalanceWorker) ReBalanceSet(cluster, zoneName string, rType RebalanceType, highRatio, lowRatio, goalRatio float64,
	maxBatchCount int, MigrateLimitPerDisk int) error {
	ctrl, err := rw.getZoneCtrl(cluster, zoneName, rType)
	if err != nil {
		return err
	}
	err = ctrl.UpdateRatio(highRatio, lowRatio, goalRatio)
	if err != nil {
		return err
	}
	if maxBatchCount > 0 {
		ctrl.SetClusterMaxBatchCount(maxBatchCount)
	}
	ctrl.SetMigrateLimitPerDisk(MigrateLimitPerDisk)
	return nil
}

func (rw *ReBalanceWorker) ResetZoneMap() {
	rw.reBalanceCtrlMap.Range(func(key, value interface{}) bool {
		ctrl := value.(*ZoneReBalanceController)
		if ctrl.Status() == StatusStop {
			rw.reBalanceCtrlMap.Delete(key)
		}
		return true
	})
}

//type:meta or data
func (rw *ReBalanceWorker) newZoneCtrl(clusterHost, zoneName string, rType RebalanceType, maxBatchCount int,
	highRatio, lowRatio, goalRatio float64, migrateLimitPerDisk, dstMetaNodeMaxPartitionCount int) (ctrl *ZoneReBalanceController, err error) {
	ctrl, err = rw.getZoneCtrl(clusterHost, zoneName, rType)
	var rInfo *RebalancedInfoTable
	if err == nil {
		if err = ctrl.UpdateRatio(highRatio, lowRatio, goalRatio); err != nil {
			return
		}
		ctrl.dstMetaNodeMaxPartitionCount = dstMetaNodeMaxPartitionCount
		if rInfo, err = rw.insertOrUpdateRebalancedInfo(clusterHost, zoneName, rType, maxBatchCount,
			highRatio, lowRatio, goalRatio, migrateLimitPerDisk, dstMetaNodeMaxPartitionCount, StatusRunning); err != nil {
			return
		}
		//ctrl, err = rw.getZoneCtrl(clusterHost, zoneName)
		//if err != nil {
		//	return
		//}
		ctrl.Id = rInfo.ID
		return
	}
	rInfo, err = rw.GetRebalancedInfoByHostAndZoneName(clusterHost, zoneName, rType)
	if err != nil && err.Error() != RECORD_NOT_FOUND {
		return
	}
	//if rInfo != nil && rInfo.ID > 0 && rInfo.Status == int(StatusRunning) {
	//	err = fmt.Errorf("rebalance already exists")
	//	return
	//}
	if rInfo, err = rw.insertOrUpdateRebalancedInfo(clusterHost, zoneName, rType, maxBatchCount,
		highRatio, lowRatio, goalRatio, migrateLimitPerDisk, dstMetaNodeMaxPartitionCount, StatusRunning); err != nil {
		return
	}
	ctrl, err = NewZoneReBalanceController(rInfo.ID, clusterHost, zoneName, rType, highRatio, lowRatio, goalRatio, dstMetaNodeMaxPartitionCount, rw)
	if err != nil {
		return
	}
	ctrl.SetCreatedUpdatedAt(rInfo.CreatedAt, rInfo.UpdatedAt)
	rw.reBalanceCtrlMap.Store(path.Join(clusterHost, zoneName, rType.String()), ctrl)
	return
}

func (rw *ReBalanceWorker) getZoneCtrl(clusterHost, zoneName string, rType RebalanceType) (*ZoneReBalanceController, error) {
	if res, ok := rw.reBalanceCtrlMap.Load(path.Join(clusterHost, zoneName, rType.String())); !ok {
		return nil, fmt.Errorf("get zone rebalance controller error with cluster:%v zoneName:%v", clusterHost, zoneName)
	} else {
		ctrl := res.(*ZoneReBalanceController)
		return ctrl, nil
	}
}

func (rw *ReBalanceWorker) doReleaseZone(cluster, zoneName string) error {
	dataNodes, err := getZoneDataNodesByClusterName(cluster, zoneName)
	if err != nil {
		return err
	}
	for _, node := range dataNodes {
		if err = rw.doReleaseDataNodePartitions(node, ""); err != nil {
			log.LogErrorf("release dataNode error cluster: %v zone: %v dataNode %v %v", cluster, zoneName, node, err)
		}
	}
	return nil
}

func (rw *ReBalanceWorker) doReleaseDataNodePartitions(dataNodeHttpAddr, timeLocation string) (err error) {
	var (
		data   []byte
		reqURL string
		key    string
	)
	if timeLocation == "" {
		key = generateAuthKey()
	} else {
		key = generateAuthKeyWithTimeZone(timeLocation)
	}
	dataHttpClient := http_client.NewDataClient(dataNodeHttpAddr, false)
	_, err = dataHttpClient.ReleasePartitions(key)
	if err != nil {
		return fmt.Errorf("url[%v],err %v resp[%v]", reqURL, err, string(data))
	}
	log.LogInfof("action[doReleaseDataNodePartitions] url[%v] resp[%v]", reqURL, string(data))
	return
}

func generateAuthKey() string {
	date := time.Now().Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func generateAuthKeyWithTimeZone(timeLocation string) string {
	var t time.Time
	if timeLocation == "" {
		t = time.Now()
	} else {
		l, _ := time.LoadLocation(timeLocation)
		t = time.Now().In(l)
	}
	date := t.Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}
