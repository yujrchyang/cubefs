package fs

import (
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/errors"
)

var (
	readConnTimeout = 3 * int64(time.Second)
)

func Test_EnableJdosKernelWriteBack(t *testing.T) {
	s := &Super{}
	// create control file
	JdosKernelWriteBackControlFile = "/tmp/enable_fuse_cgwb"
	defer func() {
		JdosKernelWriteBackControlFile = "/proc/sys/kernel/enable_fuse_cgwb"
	}()
	f, err := os.Create(JdosKernelWriteBackControlFile)
	if err != nil {
		t.Fatalf("Test_EnableJdosKernelWriteBack: create control file(%v) err(%v)", JdosKernelWriteBackControlFile, err)
		return
	}
	f.Close()

	tests := []struct {
		name              string
		volWriteCache     bool
		expectFileContent string
	}{
		{
			"test_on",
			true,
			"1",
		},
		{
			"test_off",
			false,
			"0",
		},
	}
	mc := masterSDK.NewMasterClient(ltptestMaster, false)
	volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(ltptestVolume)
	if err != nil {
		t.Fatalf("Test_EnableJdosKernelWriteBack: get vol(%v) info err(%v)", ltptestVolume, err)
		return
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// update vol write-cache
			err := mc.AdminAPI().UpdateVolume(ltptestVolume, volInfo.Capacity, int(volInfo.DpReplicaNum), int(volInfo.MpReplicaNum),
				int(volInfo.TrashRemainingDays), int(volInfo.DefaultStoreMode), volInfo.FollowerRead, false, false,
				false, false, false, false, false, tt.volWriteCache, calcAuthKey("ltptest"),
				"default", "0,0", "", 0, 0, 60, volInfo.CompactTag,
				0, 0, 0, 0, 0, volInfo.UmpCollectWay, -1, -1, false,
				"", false, false, 0, false, readConnTimeout, readConnTimeout, 0, 0, false, false)
			if err != nil {
				t.Errorf("Test_EnableJdosKernelWriteBack update vol err: %v test(%v)", err, tt)
				return
			}
			// write control file
			var info *proto.VolStatInfo
			if info, err = mc.ClientAPI().GetVolumeStat(ltptestVolume); err != nil {
				err = errors.Trace(err, "Get volume stat failed, check your masterAddr!")
				return
			}
			if err = s.EnableJdosKernelWriteBack(info.EnableWriteCache); err != nil {
				t.Errorf("Test_EnableJdosKernelWriteBack write file err: %v test(%v)", err, tt)
				return
			}
			// verify file
			var (
				controlFile *os.File
				readBytes   []byte
			)
			if controlFile, err = os.OpenFile(JdosKernelWriteBackControlFile, os.O_RDONLY, 0644); err != nil {
				t.Errorf("Test_EnableJdosKernelWriteBack open file err: %v test(%v)", err, tt)
				return
			}
			readBytes = make([]byte, 1)
			if _, err = controlFile.ReadAt(readBytes, 0); err != nil {
				t.Errorf("Test_EnableJdosKernelWriteBack read file err: %v test(%v)", err, tt)
				return
			}
			if string(readBytes) != tt.expectFileContent {
				t.Errorf("Test_EnableJdosKernelWriteBack check file failed: expect(%v) but(%v) test(%v)",
					tt.expectFileContent, string(readBytes), tt)
				return
			}
		})
	}
	// test control file not exist
	os.Remove(JdosKernelWriteBackControlFile)
	if err := s.EnableJdosKernelWriteBack(true); err != nil {
		t.Fatalf("Test_EnableJdosKernelWriteBack: enable control file(%v) err(%v)", JdosKernelWriteBackControlFile, err)
		return
	}
}
