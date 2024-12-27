package service

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/proto"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"

	// 移除这个sdk
	sdk "github.com/cubefs/cubefs/sdk/graphql/client"
)

type LoginService struct {
	sync.RWMutex
	masterClients map[string]*sdk.MasterGClient
	api           *api.APIManager
}

func NewLoginService(clusters []*model.ConsoleCluster) *LoginService {
	ls := &LoginService{
		masterClients: make(map[string]*sdk.MasterGClient, 0),
		api:           api.NewAPIManager(clusters),
	}
	for _, cluster := range clusters {
		mc := sdk.NewMasterGClient(strings.Split(cluster.MasterAddrs, ","), cluster.IsRelease)
		ls.masterClients[cluster.ClusterName] = mc
	}
	return ls
}

func (ls *LoginService) SType() cproto.ServiceType {
	return cproto.LoginService
}

func (ls *LoginService) loadMasterClient(cluster string) *sdk.MasterGClient {
	ls.RLock()
	defer ls.RUnlock()

	return ls.masterClients[cluster]
}

func (ls *LoginService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()
	query := schema.Query()
	query.FieldFunc("checkLoginEnv", ls.getLoginEnv)
	query.FieldFunc("login", ls.login)
	query.FieldFunc("logout", ls.logout)
	query.FieldFunc("adminGetOpPassword", ls.getOpPassword)
	query.FieldFunc("verifyOpPassword", ls.verifyOpPassword)
	return schema.MustBuild()
}

func (ls *LoginService) DoXbpApply(apply *model.XbpApplyInfo) error {
	return nil
}

func (ls *LoginService) getLoginEnv(ctx context.Context, args struct {
}) (*LoginResponse, error) {
	log.LogInfof("checkLogin: enter")
	var boolToIntFunc = func(b bool) int8 {
		if b {
			return 1
		}
		return 0
	}
	response := &LoginResponse{
		Intranet: boolToIntFunc(cutil.Global_CFG.IsIntranet),
	}
	var user string
	if cutil.Global_CFG.IsIntranet {
		if value := ctx.Value(cutil.SSOLoginCookie); value != nil {
			ssoCookie := value.(string)
			log.LogInfof("checkLogin: cookie: %v", ssoCookie)
			if erpInfo, err := cutil.GetERPInfoByCookies(ssoCookie); err == nil {
				user = erpInfo.Username
				cutil.CookieRegister(user, ssoCookie)
				response.Registered = 1
			}
		}
	} else {
		// todo: 怎么拿user信息
		if token := ctx.Value(proto.HeadAuthorized); token != nil && token != "null" {
			response.Registered = 1
		}
	}
	if response.Registered > 0 {
		// 登陆才校验 角色 内网/外网
		response.Role = boolToIntFunc(!model.IsAdmin(user))
		//response.Role = 1
	}
	return response, nil
}

func (ls *LoginService) logout(ctx context.Context, args struct {
}) (*LoginResponse, error) {
	var (
		isIntra      int8
		redirectLink string
	)
	if cutil.Global_CFG.IsIntranet {
		isIntra = 1
		redirectLink = BuildSSOLogoutURL()
	}
	return &LoginResponse{
		Intranet:     isIntra,
		RedirectLink: redirectLink,
	}, nil
}

type UserToken struct {
	UserID   string `json:"userID"`
	Token    string `json:"token"`
	Status   bool   `json:"status"`
	UserInfo *proto.UserInfo
}

// 非内网用户  输入用户名密码时的校验接口
func (ls *LoginService) login(ctx context.Context, args struct {
	UserID   string
	Password string
	empty    bool
}) (*UserToken, error) {
	log.LogInfof("login: enter")
	if cutil.Global_CFG.IsIntranet {
		ssoCookie := ctx.Value(cutil.SSOLoginCookie).(string)
		log.LogInfof("login: cookie: %v", ssoCookie)
		erpInfo, err := cutil.GetERPInfoByCookies(ssoCookie)
		if err != nil {
			return nil, err
		}
		cutil.CookieRegister(erpInfo.Username, ssoCookie)
		return &UserToken{
			UserID: erpInfo.Username,
			Token:  ssoCookie,
		}, nil
	} else {
		// todo: remove
		cluster := cutil.GlobalCluster
		if cproto.IsRelease(cluster) {
			return &UserToken{
				UserID: args.UserID,
				Token:  cutil.TokenRegister(&proto.UserInfo{UserID: args.UserID}),
				Status: true,
			}, nil
		}
		mc := ls.loadMasterClient(cluster)
		_, err := mc.ValidatePassword(ctx, args.UserID, args.Password)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, proto.UserKey, args.UserID)
		client := ls.api.GetMasterClient(cluster)
		userInfo, err := client.UserAPI().GetUserInfo(args.UserID)
		if err != nil {
			return nil, err
		}

		return &UserToken{
			UserID: userInfo.UserID,
			Token:  cutil.TokenRegister(userInfo), //Authorization
			Status: true,
		}, nil
	}
}

func getUserInfoFromCtx(ctx context.Context) (user string) {
	if cutil.Global_CFG.IsIntranet {
		ssoCookie := ctx.Value(cutil.SSOLoginCookie).(string)
		erpInfo, err := cutil.GetERPInfoByCookies(ssoCookie)
		if err != nil {
			log.LogErrorf("getUserInfoFromCtx: getERPByCookie failed: err(%v)", err)
			return
		}
		return erpInfo.Username
	} else {
		v := ctx.Value(proto.HeadAuthorized)
		if v == nil {
			log.LogErrorf("getUserInfoFromCtx: token[%v] not in header", proto.HeadAuthorized)
			return
		}
		token := v.(string)
		// todo: 校验token
		return token
	}
}

type GetPasswordResp struct {
	Password string
}

// 啥参数也不用要 用cookies 找erp， erp再去查用户表， 查完再生成密码 插入库 再返回（两张表）
func (ls *LoginService) getOpPassword(ctx context.Context, args struct {
}) (*GetPasswordResp, error) {
	// for test
	//password := cutil.RandomKey(16)
	//resp := &GetPasswordResp{
	//	Password: password,
	//}
	// 正式
	user_pin := getUserInfoFromCtx(ctx)
	if user_pin == "" {
		return nil, fmt.Errorf("无法识别的用户，请检查登陆cookie或token")
	}
	table := &model.ConsoleAdminOpPassword{}
	entry, err := table.GetLatestOpPassword(user_pin)
	if err != nil {
		return nil, err
	}
	// todo: entry是否为nil
	var password string
	if model.IsExpiredOpPassword(entry) {
		password = cutil.RandomKey(16)
		err := table.InsertAdminOpPassword(user_pin, password)
		if err != nil {
			return nil, fmt.Errorf("获取密码失败：插入失败！")
		}
	} else {
		password = entry.OpPassword
	}
	resp := &GetPasswordResp{
		Password: password,
	}
	return resp, nil
}

type VerifyResponse struct {
	Passed bool
	Msg    string
}

func (ls *LoginService) verifyOpPassword(ctx context.Context, args struct {
	Password string
}) (*VerifyResponse, error) {
	// for test
	//valid := args.Password != "" && args.Password != "null"
	//resp := &VerifyResponse{
	//	Passed: valid,
	//}
	// 正式
	user_pin := getUserInfoFromCtx(ctx)
	if user_pin == "" {
		return nil, fmt.Errorf("无法识别的用户，请检查登陆cookie或token")
	}
	table := &model.ConsoleAdminOpPassword{}
	entry, err := table.GetLatestOpPassword(user_pin)
	if err != nil {
		return nil, err
	}
	resp := &VerifyResponse{}
	resp.Passed = entry.OpPassword == args.Password
	if !resp.Passed {
		resp.Msg = "密码校验不通过"
	}
	return resp, nil
}

type permissionMode int

const ADMIN permissionMode = permissionMode(1)
const USER permissionMode = permissionMode(2)

// todo：erp信息的校验, 用表
func permissions(ctx context.Context, mode permissionMode) (userInfo *proto.UserInfo, perm permissionMode, err error) {
	userInfo = ctx.Value(proto.UserInfoKey).(*proto.UserInfo)

	perm = USER
	if userInfo.UserType == proto.UserTypeRoot || userInfo.UserType == proto.UserTypeAdmin {
		perm = ADMIN
	}

	if ADMIN&mode == ADMIN {
		if perm == ADMIN {
			return
		}
	}

	if USER&mode == USER {
		if perm == USER {
			return
		}
	}
	perm = permissionMode(0)
	err = fmt.Errorf("user:[%s] permissions has err:[%d] your:[%d]", userInfo.UserID, mode, perm)
	userInfo = nil
	return
}

type LoginResponse struct {
	Intranet     int8   `json:"intranet"` //内：1 外：0
	RedirectLink string `json:"redirectLink"`
	UrlSuffix    string `json:"urlSuffix"`
	Cookies      string `json:"cookies"`
	Registered   int8   `json:"registered"` // 0-已登陆
	Role         int8   `json:"role"`       // 用户角色 0-管理员 1-普通
}

func NewLoginResponse(redirect string) *LoginResponse {
	return &LoginResponse{
		RedirectLink: redirect,
	}
}

func BuildSSOLoginUrl() string {
	return fmt.Sprintf("http://%s%s?ReturnUrl=http://%s/", cutil.Global_CFG.SSOConfig.Domain, cutil.SSOLoginPath, cutil.Global_CFG.LocalIP)
}

func BuildSSOLogoutURL() string {
	return fmt.Sprintf("http://%s%s?ReturnUrl=%s", cutil.Global_CFG.SSOConfig.Domain, cutil.SSOLogoutPath, BuildSSOLoginUrl())
}
