package data

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
)

const (
	ltptestVolume = "ltptest"
)

var (
	ltptestMaster = []string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"}
	mw            *meta.MetaWrapper
	ec            *ExtentClient
	mc            *masterSDK.MasterClient

	ctx         = context.Background()
	letterRunes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func TestMain(m *testing.M) {
	tearDown := setUp()
	m.Run()
	tearDown()
}

func setUp() func() {
	_, err := log.InitLog("/cfs/log", "unittest", log.InfoLevel, nil)
	if err != nil {
		fmt.Println("init log in /cfs/log failed")
	}
	mw, ec, _ = creatExtentClient()
	mc = masterSDK.NewMasterClient(ltptestMaster, false)
	return func() {
		ec.Close(nil)
		log.LogFlush()
		os.RemoveAll("/cfs/mnt")
	}
}

func TestInit(t *testing.T) {
	assert.NotNil(t, mw)
	assert.NotNil(t, ec)
}

func creatExtentClient() (mw *meta.MetaWrapper, ec *ExtentClient, err error) {
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        ltptestVolume,
		Masters:       ltptestMaster,
		ValidateOwner: true,
		Owner:         ltptestVolume,
	}); err != nil {
		fmt.Printf("NewMetaWrapper failed: err(%v) vol(%v)", err, ltptestVolume)
		return
	}
	ic := cache.NewInodeCache(1*time.Minute, 100, 1*time.Second, true, nil)
	if ec, err = NewExtentClient(&ExtentConfig{
		Volume:            ltptestVolume,
		Masters:           ltptestMaster,
		FollowerRead:      false,
		MetaWrapper:       mw,
		OnInsertExtentKey: mw.InsertExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		OnPutIcache:       ic.Put,
		TinySize:          NoUseTinyExtent,
		UseLastExtent:     true,
	}, nil); err != nil {
		fmt.Printf("NewExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
		return
	}
	return
}

func create(name string) (*proto.InodeInfo, error) {
	mw.Delete_ll(ctx, proto.RootIno, name, false)
	return mw.Create_ll(nil, proto.RootIno, name, 0644, 0, 0, nil)
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}

func randTestData(size int) (data []byte) {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, size)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return b
}
