package fs

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
)

const (
	ltptestVolume = "ltptest"
	ltptestOwner  = "ltptest"
)

var (
	ltptestMaster = []string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"}

	mw *meta.MetaWrapper
	ec *data.ExtentClient
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

func creatExtentClient() (mw *meta.MetaWrapper, ec *data.ExtentClient, err error) {
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        ltptestVolume,
		Masters:       ltptestMaster,
		ValidateOwner: true,
		Owner:         ltptestOwner,
	}); err != nil {
		fmt.Printf("NewMetaWrapper failed: err(%v) vol(%v)", err, ltptestVolume)
		return
	}
	ic := cache.NewInodeCache(1*time.Minute, 100, 1*time.Second, true)
	if ec, err = data.NewExtentClient(&data.ExtentConfig{
		Volume:            ltptestVolume,
		Masters:           ltptestMaster,
		FollowerRead:      false,
		MetaWrapper:       mw,
		OnInsertExtentKey: mw.InsertExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		OnPutIcache:       ic.Put,
		TinySize:          data.NoUseTinyExtent,
	}, nil); err != nil {
		fmt.Printf("NewExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
	}
	return
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
