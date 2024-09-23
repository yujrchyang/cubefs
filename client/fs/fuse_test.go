package fs

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/stretchr/testify/assert"
)

func TestInodeReuse(t *testing.T) {
	fileName := "TestInodeReuseFile"
	filePath := "/cfs/mnt/" + fileName
	dirName := "TestInodeReuseDir"
	dirPath := "/cfs/mnt/" + dirName
	os.Create(filePath)
	fInfo, _ := os.Stat(filePath)
	ino := fInfo.Sys().(*syscall.Stat_t).Ino
	_, err := mw.Delete_ll(nil, proto.RootIno, fileName, false)
	assert.Nil(t, err)
	err = mw.Evict(nil, ino, true)
	assert.Nil(t, err)

	mc := masterSDK.NewMasterClient(ltptestMaster, false)
	mps, err := mc.ClientAPI().GetMetaPartitions(ltptestVolume)
	assert.Nil(t, err)
	mp := getMpByInode(mps, ino)
	metaClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(mp.LeaderAddr, ":")[0], 17220), false)
	mode := uint32(fs.ModeDir | fs.ModePerm)
	err = metaClient.CreateInode(mp.PartitionID, ino, mode)
	assert.Nil(t, err)
	inodeInfo, err := metaClient.GetInode(mp.PartitionID, ino)
	assert.Nil(t, err)
	t.Logf("CreateInode mp(%v) ino(%v) mode(%v) inodeInfo(%v)", mp.PartitionID, ino, mode, inodeInfo)
	mpParent := getMpByInode(mps, proto.RootIno)
	err = mw.DentryCreate_ll(nil, mpParent.PartitionID, dirName, ino, mode)
	assert.Nil(t, err)

	exec.Command("curl", "http://192.168.0.10:17410/clearCache").Run()
	_, err = os.ReadDir(dirPath)
	assert.Nil(t, err)
}

func getMpByInode(mps []*proto.MetaPartitionView, inode uint64) *proto.MetaPartitionView {
	for _, mp := range mps {
		if inode >= mp.Start && inode < mp.End {
			return mp
		}
	}
	return nil
}

func TestRenameOpenedFile(t *testing.T) {
	testFile1 := "/cfs/mnt/TestStreamer_RenameOpenedFile1"
	testFile2 := "/cfs/mnt/TestStreamer_RenameOpenedFile2"
	os.Create(testFile1)
	info1, _ := os.Stat(testFile1)
	ino1 := info1.Sys().(*syscall.Stat_t).Ino
	os.Create(testFile2)
	err := os.Rename(testFile2, testFile1)
	assert.Nil(t, err)
	_, err = mw.InodeGet_ll(nil, ino1)
	assert.Nil(t, err)

	testName3 := "TestStreamer_RenameOpenedFile3"
	testFile3 := "/cfs/mnt/" + testName3
	info3, _ := mw.Create_ll(nil, proto.RootIno, testName3, 0777, 0, 0, nil)
	err = os.Rename(testFile1, testFile3)
	assert.Nil(t, err)
	_, err = mw.InodeGet_ll(nil, info3.Inode)
	assert.Equal(t, syscall.ENOENT, err)
}

func TestMknod(t *testing.T) {
	fileName := "/cfs/mnt/TestMknod"
	err := syscall.Mknod(fileName, 0600, 0)
	assert.Nil(t, err)
	f, _ := os.OpenFile(fileName, os.O_RDWR, 0)
	data := []byte("abc")
	f.Write(data)
	readData := make([]byte, 3)
	f.Seek(0, 0)
	f.Read(readData)
	f.Close()
	assert.Equal(t, data, readData)
}
