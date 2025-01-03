package cutil

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
)

const (
	SSOLoginPath        = "/sso/login"
	SSOLogoutPath       = "/sso/logout"
	SSOGetTicketPath    = "/sso/ticket/getTicket"
	SSOVerifyTicketPath = "/sso/ticket/verifyTicket"
)

type SSOReply struct {
	Flag bool            `json:"REQ_FLAG"`
	Code int32           `json:"REQ_CODE"`
	Msg  string          `json:"REQ_MSG"`
	Data json.RawMessage `json:"REQ_DATA"`
}

type SSOERPInfo struct {
	Email      string `json:"email"`
	Username   string `json:"username"`
	Fullname   string `json:"fullname"`
	Mobile     string `json:"mobile"`
	OrgName    string `json:"orgName"`
	Expire     uint64 `json:"expire"`
	HrmDeptId  string `json:"hrmDeptId"`
	OrgId      string `json:"orgId"`
	PersonId   string `json:"personId"`
	TenantCode string `json:"tenantCode"`
	UserId     int64  `json:"userId"`
}

func GetCookiesByTicket(ticket string) (string, error) {
	path := fmt.Sprintf("https://%s%s", Global_CFG.SSOConfig.Domain, SSOGetTicketPath)
	request := NewAPIRequest(http.MethodGet, path)
	request.AddHeader("Accept", "application/json")
	request.AddParam("sso_service_ticket", ticket)
	request.AddParam("url", Global_CFG.LocalIP)
	request.AddParam("ip", Global_CFG.LocalIP)

	body, err := SendSimpleRequest(request, true)
	if err != nil {
		log.LogError("GetTicket: send request err(%v)", err)
		return "", err
	}
	reply := new(SSOReply)
	if err = json.Unmarshal(body, &reply); err != nil {
		log.LogErrorf("GetERPInfoByCookies: unmarshal err(%v)", err)
		return "", err
	}
	if !reply.Flag {
		err = fmt.Errorf("%v", reply.Msg)
		log.LogErrorf("GetERPInfoByCookies: err(%v)", err)
		return "", err
	}

	return string(reply.Data), nil
}

func GetERPInfoByCookies(cookies string) (*SSOERPInfo, error) {
	path := fmt.Sprintf("https://%s%s", Global_CFG.SSOConfig.Domain, SSOVerifyTicketPath)
	request := NewAPIRequest(http.MethodGet, path)
	request.AddHeader("Accept", "application/json")
	request.AddParam("ticket", cookies)
	request.AddParam("url", Global_CFG.LocalIP)
	request.AddParam("ip", Global_CFG.LocalIP)

	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("GetERPInfoByCookies failed: err(%v)", err)
		}
	}()

	body, err := SendSimpleRequest(request, true)
	if err != nil {
		return nil, err
	}
	reply := new(SSOReply)
	if err = json.Unmarshal(body, &reply); err != nil {
		return nil, err
	}

	erpInfo := new(SSOERPInfo)
	if !reply.Flag {
		err = fmt.Errorf("%v", reply.Msg)
		return nil, err
	}
	if err = json.Unmarshal(reply.Data, &erpInfo); err != nil {
		return nil, err
	}

	return erpInfo, nil
}
