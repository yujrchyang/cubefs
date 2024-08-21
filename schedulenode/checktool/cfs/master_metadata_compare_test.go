package cfs

import (
	"context"
	"github.com/cubefs/cubefs/util/log"
	"os"
	"path"
	"testing"
)

func TestCompareMeta(t *testing.T) {
	t.Skipf("skip online ip")
	logdir := path.Join(os.TempDir(), "compare_test_log")
	os.RemoveAll(logdir)
	os.MkdirAll(logdir, 0666)
	log.InitLog(logdir, "master_compare", log.DebugLevel, nil)
	defer func() {
		log.LogFlush()
	}()
	monitor := NewChubaoFSMonitor(context.Background())
	monitor.configMap[cfgKeyOssDomain] = "storage-ops.x.x.x"
	monitor.chubaoFSMasterNodes = map[string][]string{
		"sparkchubaofs.jd.local": {
			/*			"1.1.1.1:8868",
						"1.1.1.2:8868",
						"1.1.1.3:8868",
						"1.1.1.4:8868",
						"1.1.1.5:8868",*/
		},
		"cn.chubaofs-seqwrite.jd.local": {
			/*			"1.1.1.1:8868",
						"1.1.1.2:8868",
						"1.1.1.3:8868",
						"1.1.1.4:8868",
						"1.1.1.5:8868",*/
		},
		"cn.elasticdb.jd.local": {},
	}
	monitor.checkMasterMetadata()
}
