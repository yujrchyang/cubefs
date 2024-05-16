package migration

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"strings"
	"testing"
)

func TestGetMpView(t *testing.T) {
	cw := NewWorker(proto.WorkerTypeCompact)
	if _, err := cw.GetMpView(clusterName, ltptestVolume); err == nil {
		t.Fatalf("GetMpView should hava err")
	}
	cw.masterClients.Store(clusterName, master.NewMasterClient(strings.Split(ltptestMaster, ","), false))
	if _, err := cw.GetMpView(clusterName, ltptestVolume); err != nil {
		t.Fatalf("GetMpView should not hava err, but err:%v", err)
	}
}

func TestGetCompactVolumes(t *testing.T) {
	mc := master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	if _, err := GetCompactVolumes(clusterName, mc); err != nil {
		t.Fatalf("GetCompactVolumes should not hava err, but err:%v", err)
	}
}

func TestGetSmartVolumes(t *testing.T) {
	mc := master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	if _, err := GetSmartVolumes(clusterName, mc); err != nil {
		t.Fatalf("GetSmartVolumes should not hava err, but err:%v", err)
	}
}