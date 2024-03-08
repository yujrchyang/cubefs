package fs

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/pkg/xattr"
	"github.com/stretchr/testify/assert"
)

func Test_TruncateWithoutOpen(t *testing.T) {
	testName := "Test_TruncateWithoutOpen"
	testPath := fmt.Sprintf("/cfs/mnt/%s", testName)
	mw.Create_ll(nil, proto.RootIno, testName, 0666, 0, 0, nil)
	size := int64(123)
	err := os.Truncate(testPath, size)
	assert.Nil(t, err)
	fInfo, _ := os.Stat(testPath)
	assert.True(t, fInfo.Size() == size)

	file, _ := os.OpenFile(testPath, os.O_RDWR, 0666)
	defer file.Close()
	err = file.Truncate(0)
	assert.Nil(t, err)
	fInfo, _ = os.Stat(testPath)
	assert.True(t, fInfo.Size() == 0)
}

func Test_Flock(t *testing.T) {
	path := "/cfs/mnt/Test_Flock"
	os.Mkdir(path, 0755)

	flock := proto.XAttrFlock{WaitTime: 1, Flag: proto.XATTR_FLOCK_FLAG_RECURSIVE | proto.XATTR_FLOCK_FLAG_WRITE, IpPort: "192.168.0.10:17410"}
	data, _ := json.Marshal(flock)
	err := xattr.Set(path, proto.XATTR_FLOCK, data)
	assert.Nil(t, err)
	err = os.Mkdir(path+"/d1", 0666)
	assert.Nil(t, err)
	f, err := os.Create(path + "/d1/f1")
	_, err = f.Write([]byte("abc"))
	assert.Nil(t, err)
	f.Close()
	err = os.Remove(path + "/d1/f1")
	assert.Nil(t, err)
	err = os.Remove(path + "/d1")
	assert.Nil(t, err)
}
