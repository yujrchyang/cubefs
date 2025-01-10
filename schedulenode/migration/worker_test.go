package migration

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	region     = "chubaofs01"
	endPoint   = "http://object.chubao.io"
	accessKey  = "39bEF4RrAQgMj6RV"
	secretKey  = "TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd"
	bucketName = "ltptest"
)

func TestCheckVolumeForceROW(t *testing.T) {
	smartRules := "inodeAccessTime:timestamp:1681979665:hdd"
	forceRowModifyTime := "2000-12-01 00:00:00"
	tt, err := time.Parse(proto.TimeFormat, forceRowModifyTime)
	assert.Nil(t, err)
	volumeInfo := &proto.SimpleVolView{
		ForceROW:           true,
		ForceROWModifyTime: tt.Unix(),
	}
	err = checkVolumeForceROW(smartRules, volumeInfo)
	assert.Nil(t, err)
	smartRules = "inodeAccessTime:timestamp:1681979665:s3"
	err = checkVolumeForceROW(smartRules, volumeInfo)
	assert.Nil(t, err)
	volumeInfo.ForceROWModifyTime = time.Now().Unix()
	err = checkVolumeForceROW(smartRules, volumeInfo)
	expectErr := fmt.Errorf("forcerow has been opened, reset s3 migration after %v",
		time.Unix(volumeInfo.ForceROWModifyTime+ForceROWModifyDuration, 0).Format(proto.TimeFormat))
	assert.EqualError(t, err, expectErr.Error())
	volumeInfo.ForceROW = false
	err = checkVolumeForceROW(smartRules, volumeInfo)
	expectErr = fmt.Errorf("please open forcerow first")
	assert.EqualError(t, err, expectErr.Error())
}

func TestCheckS3BucketExist(t *testing.T) {
	smartRules := "inodeAccessTime:timestamp:1681979665:hdd"
	volumeInfo := &proto.SimpleVolView{
		BoundBucket: &proto.BoundBucketInfo{
			EndPoint:   "",
			BucketName: "",
		},
	}
	err := checkS3BucketExist(smartRules, volumeInfo)
	assert.Nil(t, err)
	smartRules = "inodeAccessTime:timestamp:1681979665:s3"
	err = checkS3BucketExist(smartRules, volumeInfo)
	expectErr := fmt.Errorf("s3 configuration does not exist")
	assert.EqualError(t, err, expectErr.Error())
	volumeInfo.BoundBucket = &proto.BoundBucketInfo{
		Region:          region,
		EndPoint:        endPoint,
		AccessKey:       accessKey,
		SecretAccessKey: secretKey,
		BucketName:      bucketName,
	}
	err = checkS3BucketExist(smartRules, volumeInfo)
	assert.Nil(t, err)
	volumeInfo.BoundBucket.BucketName = bucketName + "NoExist"
	err = checkS3BucketExist(smartRules, volumeInfo)
	expectErr = fmt.Errorf("bucket(%v) is not exist", volumeInfo.BoundBucket.BucketName)
	assert.EqualError(t, err, expectErr.Error())
}
