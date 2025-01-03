package meta

import (
	"context"
	"fmt"
	"testing"

	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
)

const (
	ltptestVolume = "ltptest"
)

var (
	ltptestMaster = []string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"}
	mw            *MetaWrapper
	mc            *masterSDK.MasterClient
	ctx           = context.Background()
	letterRunes   = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func TestMain(m *testing.M) {
	setUp()
	m.Run()
	tearDown()
}

func setUp() {
	_, err := log.InitLog("/cfs/log", "unittest", log.WarnLevel, nil)
	if err != nil {
		fmt.Println("init log in /cfs/log failed")
	}
	mw, _ = createMetaWrapper()
	mc = masterSDK.NewMasterClient(ltptestMaster, false)
}

func tearDown() {
	log.LogFlush()
}

func TestInit(t *testing.T) {
	assert.NotNil(t, mw)
}

func createMetaWrapper() (mw *MetaWrapper, err error) {
	if mw, err = NewMetaWrapper(&MetaConfig{
		Volume:        ltptestVolume,
		Masters:       ltptestMaster,
		ValidateOwner: true,
		Owner:         ltptestVolume,
	}); err != nil {
		fmt.Printf("NewMetaWrapper failed: err(%v) vol(%v)", err, ltptestVolume)
		return
	}
	return
}

func create(name string) (*proto.InodeInfo, error) {
	mw.Delete_ll(ctx, proto.RootIno, name, false)
	return mw.Create_ll(ctx, proto.RootIno, name, 0644, 0, 0, nil)
}
