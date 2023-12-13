package fs

import (
	"fmt"
	"os"
	"testing"

	"github.com/cubefs/cubefs/proto"
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
