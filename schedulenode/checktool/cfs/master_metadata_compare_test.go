package cfs

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"testing"
)

func TestCompareMeta(t *testing.T) {
	t.Skipf("skip online ip")
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	var ln net.Listener
	var err error
	if ln, err = net.Listen("tcp", fmt.Sprintf(":%v", 6105)); err != nil {
		fmt.Printf(fmt.Sprintf("Fatal: listen prof port %v failed: %v", 6105, err))
		return
	}
	// 在prof端口监听上启动http API.
	go func() {
		_ = http.Serve(ln, http.DefaultServeMux)
	}()

	monitor := NewChubaoFSMonitor(context.Background())
	monitor.envConfig = &EnvConfig{
		JcloudOssDomain: "oss****",
	}
	monitor.chubaoFSMasterNodes = map[string][]string{
		DomainSpark: {
			/*			"1.1.1.1:8868",
						"1.1.1.2:8868",
						"1.1.1.3:8868",
						"1.1.1.4:8868",
						"1.1.1.5:8868",*/
		},
		DomainDbbak: {
			/*			"1.1.1.1:8868",
						"1.1.1.2:8868",
						"1.1.1.3:8868",
						"1.1.1.4:8868",
						"1.1.1.5:8868",*/
		},
		DomainMysql: {},
	}
	monitor.checkMasterMetadata()
	monitor.checkMasterMetadata()
}
