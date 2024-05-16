package cutil

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const (
	IdcParameterUser       = "user"
	IdcParameterTime       = "time"
	IdcParameterAuth       = "auth"
	IdcParameterRandom     = "random"
	IdcParameterApplyUser  = "apply_user"
	IdcParameterFaultList  = "faultList"
	IdcParameterApplyField = "field"
	IdcParameterOrderId    = "order_id"

	User          = "api_img"
	IDCToken      = "f2699a868181c0095a351370759c826e"
	IDCOnlineSite = "http://idc.jd.com"
)

type IdcResult struct {
	Msg  string          `json:"message"`
	Code int             `json:"status_code"`
	Data json.RawMessage `json:"data"`
}

type OrderBaseInfo struct {
	OrderId     int    `json:"order_id"`
	OrderSn     string `json:"order_sn"`
	OrderStatus string `json:"order_status_name"`
}

type OrderDetail struct {
	BaseInfo *OrderBaseInfo `json:"base_info"`
}

func GetOrderStatus(orderId int) string {
	if orderId <= 0 {
		return ""
	}
	idcOpenApi := NewIdcFaultApi()
	result, err := idcOpenApi.IdcFaultDetail(orderId)
	if err != nil || result.BaseInfo == nil || result.BaseInfo.OrderStatus == "" {
		return "暂无"
	}
	return result.BaseInfo.OrderStatus
}

type IDCOpenApi struct {
	openAPIURL string
	httpSend   *HttpRequest
}

func NewIdcFaultApi() *IDCOpenApi {
	iDCOpenApi := new(IDCOpenApi)
	iDCOpenApi.openAPIURL = IDCOnlineSite + "/v1.0/fault/detail"
	iDCOpenApi.httpSend = NewAPIRequest(http.MethodPost, iDCOpenApi.openAPIURL)
	iDCOpenApi.httpSend.AddHeader("Accept", "application/json")
	return iDCOpenApi
}

func (api *IDCOpenApi) IdcFaultDetail(orderId int) (detailResult *OrderDetail, err error) {
	auth := generateAuth()
	body := map[string]interface{}{
		IdcParameterUser:    User,
		IdcParameterTime:    strconv.FormatInt(time.Now().Unix(), 10),
		IdcParameterAuth:    auth,
		IdcParameterRandom:  strconv.FormatInt(time.Now().Unix()*1000, 10),
		IdcParameterOrderId: orderId,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	api.httpSend.AddBody(data)

	bytes, err := SendSimpleRequest(api.httpSend, true)
	if err != nil {
		return
	}

	rt := &IdcResult{}
	if err = json.Unmarshal(bytes, &rt); err != nil {
		err = fmt.Errorf("Unmarshal fault detail result failed!")
		return
	}
	if rt.Code != 200 {
		err = fmt.Errorf(rt.Msg)
		return
	}

	detailResult = &OrderDetail{}
	if err = json.Unmarshal([]byte(rt.Data), &detailResult); err != nil {
		err = fmt.Errorf("Unmarshal create fault result failed!")
		return
	}

	return
}

func generateAuth() string {
	curTime := strconv.FormatInt(time.Now().Unix(), 10)
	h := md5.New()
	h.Write([]byte(User + IDCToken + curTime))
	return hex.EncodeToString(h.Sum(nil))
}
