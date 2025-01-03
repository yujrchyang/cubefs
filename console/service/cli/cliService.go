package cli

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type CliService struct {
	api *api.APIManager
}

var (
	ErrUnSupportOperation = errors.New("unsupported operation for cluster")
	ErrEmptyInputMetrics  = errors.New("empty value metrics")
)

const (
	_ int32 = iota
	ErrCode
)

func NewCliService(cluster []*model.ConsoleCluster) *CliService {
	cli := new(CliService)
	cli.api = api.NewAPIManager(cluster)
	return cli
}

func (cli *CliService) GetDatanodeZoneList(cluster string) []string {
	return cli.api.GetDatanodeZoneList(cluster)
}

func (cli *CliService) GetFlashnodeZoneList(cluster string) []string {
	return cli.GetDatanodeZoneList(cluster)
}

func (cli *CliService) GetVolList(cluster string) []string {
	volList, err := cli.api.GetVolNameList(cluster)
	if err != nil {
		log.LogErrorf("getVolList failed: cluster[%v] err(%v)", cluster, err)
		return nil
	}
	return volList
}

func (cli *CliService) GetOperationHistory(cluster, module, operation string, page, pageSize int) (total int64, data []*cproto.CliOperationHistory, err error) {
	if cutil.Global_CFG.EnableXBP {
		total, err = model.XbpApplyInfo{}.GetXbpApplyRecordsCount(cluster, module, operation)
		if err != nil {
			return 0, nil, err
		}
		applys, err := model.XbpApplyInfo{}.GetXbpApplyRecord(cluster, module, operation, page, pageSize)
		if err != nil {
			return 0, nil, err
		}
		for _, apply := range applys {
			record := &cproto.CliOperationHistory{
				XbpTicketId: apply.TicketID,
				XbpUrl:      fmt.Sprintf("http://%s/operate/%v", strings.Replace(cutil.Global_CFG.XbpProcessConfig.Domain, "-api", "", 1), strconv.FormatUint(apply.TicketID, 10)),
				Cluster:     cluster,
				Module:      module,
				Operation:   operation,
				Params:      apply.Params,
				XbpStatus:   cproto.GetXbpStatusMessage(apply.Status),
				Pin:         apply.Pin,
				CreateTime:  apply.CreateTime.Format(time.DateTime),
				UpdateTime:  apply.UpdateTime.Format(time.DateTime),
			}
			cliRecord := model.CliOperationRecord{}.LoadRecordByTicketID(apply.TicketID)
			if cliRecord != nil {
				record.OperationStatus = cliRecord.Status
				record.Message = cliRecord.ErrMsg
				record.UpdateTime = cliRecord.CreateTime.Format(time.DateTime)
			}
			data = append(data, record)
		}
	} else {
		total, err = model.CliOperationRecord{}.GetOperationRecordCount(cluster, module, operation)
		if err != nil {
			return 0, nil, err
		}
		records, err := model.CliOperationRecord{}.GetOperationRecord(cluster, module, operation, page, pageSize)
		if err != nil {
			return 0, nil, err
		}
		for _, cliRecord := range records {
			record := &cproto.CliOperationHistory{
				Cluster:         cluster,
				Module:          module,
				Operation:       operation,
				Params:          cliRecord.Params,
				OperationStatus: cliRecord.Status,
				Pin:             cliRecord.Pin,
				Message:         cliRecord.ErrMsg,
				CreateTime:      cliRecord.CreateTime.Format(time.DateTime),
				UpdateTime:      cliRecord.UpdateTime.Format(time.DateTime),
			}
			data = append(data, record)
		}
	}
	return
}

// 创建表单可以仅几个字段，但是插数据库 可以把metrics json编码 全部存，包括类型
// 或者前面已经参数校验了，xbp回调直接请求接口？
// 区分list和非list情况
// 节点列表的xbp审批单子 不适用这个方法 重新写一个
func (cli *CliService) createXbpApply(ctx context.Context, cluster string, moduleType int, operation int, metrics [][]*cproto.CliValueMetric, volume, keyValuePath *string, isList bool) error {
	pin := ctx.Value(cutil.PinKey).(string)
	if pin == "" {
		return fmt.Errorf("创建xbp申请失败: 获取pin失败：%s", pin)
	}
	var setOperationMsg = func(moduleType int) string {
		if moduleType == cproto.KeyValueModuleType {
			return *keyValuePath
		}
		return cproto.GetOperationShortMsg(operation)
	}

	apply := cproto.NewXBPApply(cutil.Global_CFG.XbpProcessConfig.ProcessID, pin)
	apply.Add(cproto.XbpClusterKey, cluster)
	apply.Add(cproto.XbpTypeKey, "cli配置")
	apply.Add(cproto.XbpModuleKey, cproto.GetModule(moduleType))
	apply.Add(cproto.XbpOperationKey, setOperationMsg(moduleType))
	apply.Add(cproto.XbpParamsKey, fmt.Sprintf("%v", metrics))
	if volume != nil {
		apply.Add(cproto.XbpVolumeKey, *volume)
	}
	ticketID, err := sendCreateXbpRequest(apply)
	if err != nil {
		return err
	}

	record := &model.XbpApplyInfo{
		TicketID:        ticketID,
		Cluster:         cluster,
		Pin:             pin,
		ModuleType:      moduleType,
		Module:          cproto.GetModule(moduleType),
		OperationCode:   operation,
		Operation:       setOperationMsg(moduleType),
		OperationIsList: isList,
		ServiceType:     int(cproto.CliService),
	}
	if volume != nil {
		record.Volume = *volume
	}
	if body, err := json.Marshal(metrics); err != nil {
		return err
	} else {
		record.Params = string(body)
	}
	record.InsertXbpApply(record)
	return nil
}

func sendCreateXbpRequest(applyInfo *cproto.XBPApply) (ticketID uint64, err error) {
	body, err := json.Marshal(applyInfo)
	if err != nil {
		log.LogErrorf("marshal xbpApply failed: %v", err)
		return 0, err
	}
	sess, ts := session(cutil.Global_CFG.XbpProcessConfig.APISign)
	request := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", cutil.Global_CFG.XbpProcessConfig.Domain, cproto.XbpCreateTicketPath))
	request.AddHeader("Xbp-Api-User", cutil.Global_CFG.XbpProcessConfig.APIUser)
	request.AddHeader("Xbp-Api-Session", sess)
	request.AddHeader("Xbp-Api-Timestamp", ts)
	request.AddBody(body)
	data, err := cutil.SendSimpleRequest(request, false)
	if err != nil {
		log.LogErrorf("sendCreateXbpRequest failed: %v", err)
		return
	}
	resp := new(cproto.XbpCreateResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		log.LogErrorf("Unmarshal createXbp response failed: %v", err)
		return
	}
	log.LogInfof("申请xbp成功, apply: %v, ticketID[%v]", applyInfo, resp.TicketID)
	return resp.TicketID, nil
}

func session(sign string) (session string, ts string) {
	ts = fmt.Sprintf("%v", time.Now().UnixMilli())
	hash := md5.Sum([]byte(ts + sign))
	session = hex.EncodeToString(hash[:])
	return session, ts
}
