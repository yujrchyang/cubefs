package cutil

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultRootUser = "root"
	charset         = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	// todo: userCache 在内存中
	userCache = sync.Map{}
)

type userCacheEntity struct {
	info        interface{} // string && *proto.UserInfo
	expiredTime time.Time
}

func TokenValidate(token string) (*proto.UserInfo, error) {
	cache, found := userCache.Load(token)
	if !found {
		// 内网 用户带着cookie请求接口 先注册
		if !Global_CFG.IsIntranet {
			return nil, fmt.Errorf("无效token: %s！", token)
		}
		log.LogInfof("TokenValidate: cookie: %v", token)
		if erpInfo, err := GetERPInfoByCookies(token); err != nil {
			return nil, err
		} else {
			CookieRegister(erpInfo.Username, token)
			cache, _ = userCache.Load(token)
		}
	}
	value := cache.(*userCacheEntity)
	if value.expiredTime.Before(time.Now()) {
		userCache.Delete(token)
		return nil, fmt.Errorf("登陆超时，请重新登陆！")
	}
	var userInfo *proto.UserInfo
	if Global_CFG.IsIntranet {
		pin := value.info.(string)
		userInfo = &proto.UserInfo{
			UserID: pin,
		}
	} else {
		userInfo = value.info.(*proto.UserInfo)
	}
	return userInfo, nil
}

func TokenRegister(ui *proto.UserInfo) string {
	token := uuid.New().String()
	userCache.Store(token, &userCacheEntity{
		info:        ui,
		expiredTime: time.Now().Add(24 * time.Hour),
	})
	log.LogInfof("TokenRegister: user[%s] time: %s", ui.UserID, time.Now().Format(time.DateTime))
	return token
}

func CookieRegister(userPin, cookie string) {
	key := cookie
	userCache.Store(key, &userCacheEntity{
		info:        userPin,
		expiredTime: time.Now().Add(24 * time.Hour),
	})
	log.LogInfof("CookieRegister: erp[%s] %s time: %s", userPin, cookie, time.Now().Format(time.DateTime))
}

func RandomKey(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
