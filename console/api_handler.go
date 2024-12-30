package console

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	"github.com/cubefs/cubefs/console/proto"
	flow "github.com/cubefs/cubefs/console/service/flowSchedule"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"time"
)

func (c *ConsoleNode) solveXbpCallback(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			cutil.SendHttpReply(w, r, cutil.BuildHttpReply(1, err.Error()))
		} else {
			cutil.SendHttpReply(w, r, cutil.BuildHttpReply(0, "success"))
		}
	}()
	if err = r.ParseForm(); err != nil {
		return
	}
	var (
		ticketID  uint64
		typen     int
		status    int
		receiveTs int64
	)
	if value := r.FormValue(proto.XbpParamTicketID); value == "" {
		err = fmt.Errorf("param[%v] not found", proto.XbpParamTicketID)
		return
	} else {
		ticketID, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}
	}
	if value := r.FormValue(proto.XbpParamType); value == "" {
		err = fmt.Errorf("param[%v] not found", proto.XbpParamType)
		return
	} else {
		typen, err = strconv.Atoi(value)
		if err != nil {
			return
		}
	}
	if value := r.FormValue(proto.XbpParamStatus); value == "" {
		err = fmt.Errorf("param[%v] not found", proto.XbpParamStatus)
		return
	} else {
		status, err = strconv.Atoi(value)
		if err != nil {
			return
		}
	}
	if value := r.FormValue(proto.XbpParamTimestamp); value == "" {
		err = fmt.Errorf("param[%v] not found", proto.XbpParamStatus)
		return
	} else {
		receiveTs, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return
		}
	}
	if typen != 0 {
		err = fmt.Errorf("不支持的审批单类型[%v]", typen)
		return
	}
	log.LogInfof("SolveXbpCallBack: receive ticket[%v], time: %v", ticketID, time.Unix(receiveTs/1000, 0))
	err = c.xbpApplyCallback(ticketID, status)
	return
}

func (c *ConsoleNode) xbpApplyCallback(ticketID uint64, status int) (err error) {
	apply := model.XbpApplyInfo{}.LoadXbpApply(ticketID)
	if apply == nil {
		err = fmt.Errorf("can't find apply record by tickeID[%v]", ticketID)
		return
	}

	switch status {
	case proto.XBP_Withdrawn, proto.XBP_Reject:
		model.XbpApplyInfo{}.UpdateApplyStatus(ticketID, status)

	case proto.XBP_InProcessing:
		log.LogWarnf("申请单[%v]未结单", ticketID)

	case proto.XBP_Approved:
		if apply.Status == proto.XBP_InProcessing {
			//todo: 如果执行的过程中出错了 状态的变更
			model.XbpApplyInfo{}.UpdateApplyStatus(ticketID, proto.XBP_UnderDeploy)
			err = c.getServiceBySType(proto.ServiceType(apply.ServiceType)).DoXbpApply(apply)
		}
		if apply.Status == proto.XBP_UnderDeploy || apply.Status == proto.XBP_Approved {
			// 如果表中状态为 在执行（3）和执行完成（1）  -- 直接返回
			return
		}

	default:
		err = fmt.Errorf("非法xbp状态码：%v", status)
	}
	return err
}

func addConsoleUserHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			cutil.SendHttpReply(w, r, cutil.BuildHttpReply(1, err.Error()))
		} else {
			cutil.SendHttpReply(w, r, cutil.BuildHttpReply(0, "success"))
		}
	}()
	if err = r.ParseForm(); err != nil {
		return
	}
	userPin := r.FormValue("user")
	roleStr := r.FormValue("role")
	if userPin == "" || roleStr == "" {
		err = fmt.Errorf("请输入user和role")
		return
	}
	role, err := strconv.Atoi(roleStr)
	if err != nil {
		return
	}
	if role != 0 && role != 1 {
		err = fmt.Errorf("请指定管理员-0 普通成员-1")
		return
	}
	userInfo, _ := model.ConsoleUserInfo{}.GetUserInfoByUser(userPin)
	if userInfo != nil {
		err = model.ConsoleUserInfo{}.UpdateUserRole(userPin, int8(role))
	} else {
		err = model.ConsoleUserInfo{}.InsertConsoleUser(userPin, int8(role))
	}
	return
}

func offlineDeleteRecords(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			cutil.SendHttpReply(w, r, cutil.BuildHttpReply(1, err.Error()))
		} else {
			cutil.SendHttpReply(w, r, cutil.BuildHttpReply(0, "success"))
		}
	}()
	if err = r.ParseForm(); err != nil {
		return
	}
	date := r.FormValue("date")
	daysStr := r.FormValue("days")
	cluster := r.FormValue("cluster")
	if date == "" || daysStr == "" {
		err = fmt.Errorf("请输入date和days")
		return
	}
	end, err := time.Parse(time.DateOnly, date)
	if err != nil {
		return
	}
	days, err := strconv.Atoi(daysStr)
	if err != nil {
		return
	}
	// delete one by one hour
	var deleteByHour = func(end time.Time) (final time.Time, err error) {
		for i := 0; i < 24*6; i++ {
			start := end.Add(-10 * time.Minute)
			err = cutil.CONSOLE_DB.Where("update_time >= ? AND update_time < ? AND cluster = ?", start, end, cluster).
				Delete(&model.ConsoleVolume{}).
				Error
			if err != nil {
				return time.Time{}, err
			}
			end = start
		}
		return end, nil
	}

	for {
		end, err = deleteByHour(end)
		if err != nil {
			return
		}
		days--
		if days <= 0 {
			return
		}
	}
}

func offlineQueryClientMonitorData(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			cutil.SendHttpReply(w, r, cutil.BuildHttpReply(1, err.Error()))
		}
	}()
	if err = r.ParseForm(); err != nil {
		return
	}
	cluster := r.FormValue("cluster")
	volume := r.FormValue("volume")
	ip := r.FormValue("ip")
	action := r.FormValue("action")
	if cluster == "" || volume == "" || ip == "" || action == "" {
		err = fmt.Errorf("请输入cluster,volume,ip,action")
		return
	}

	zone := r.FormValue("zone")
	startS := r.FormValue("start")
	endS := r.FormValue("end")
	intervalStr := r.FormValue("interval")

	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		err = fmt.Errorf("错误的interval类型:%v err:%v", intervalStr, err)
		return
	}
	start, err := strconv.ParseInt(startS, 10, 64)
	if err != nil || start == 0 {
		err = fmt.Errorf("错误的start:%v err:%v", startS, err)
		return
	}
	end, err := strconv.ParseInt(endS, 10, 64)
	if err != nil || end == 0 {
		err = fmt.Errorf("错误的end:%v err:%v", endS, err)
		return
	}
	req := &proto.TrafficRequest{
		ClusterName:   cluster,
		VolumeName:    volume,
		IpAddr:        ip,
		OperationType: action,
		Zone:          zone,
		IntervalType:  interval,
		StartTime:     start,
		EndTime:       end,
	}

	flowCtrl := flow.NewFlowSchedule(cutil.Global_CFG.ClickHouseConfig.User, cutil.Global_CFG.ClickHouseConfig.Password, cutil.Global_CFG.ClickHouseConfig.Host)
	result, err := flowCtrl.ListClientMonitorData(req)
	if err != nil {
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		return
	}
	cutil.SendHttpReply(w, r, &cutil.HTTPReply{
		Code: 0,
		Msg:  "success",
		Data: data,
	})
}
