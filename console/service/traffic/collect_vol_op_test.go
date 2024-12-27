package traffic

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	"sync"
	"testing"
)

func initClickHouseDBConfig() {
	cutil.ClickHouseDBHostAddr = "ckpub150.olap.jd.com:2000"
	cutil.ClickHouseDBROnlyUser = "read_chubaofs_sre_query"
	cutil.ClickHouseDBPassword = "0uMsA2BnpxMzh1SxdOzA"
}

func TestQueryVolumeOps(t *testing.T) {
	initClickHouseDBConfig()

	req := &cproto.QueryVolOpsRequest{
		Cluster: "spark",
		Period:  model.ZombiePeriodDay,
		Module:  cproto.RoleNameDataNode,
		Action:  "",
	}

	res, err := queryVolumeOps(req)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("queryVolumeOps: len(result) = %d\n", len(res))
}

func Test_RecordVolumeOps(t *testing.T) {
	recordCh := make(chan []*model.ConsoleVolumeOps, 10)
	go recordVolumeOps(recordCh)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	getVolumeOps("spark", wg, recordCh)
}
