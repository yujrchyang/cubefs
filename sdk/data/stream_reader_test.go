package data

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

func TestReadFromS3(t *testing.T)  {
	// update s3 config
	volID := uint64(99)
	clusterName := "chubaofs01"
	w := &Wrapper{clusterName: clusterName, volName: ltptestVolume, volID: volID}
	bucketInfo := &proto.BoundBucketInfo{
		EndPoint:        "http://object.chubao.io",
		BucketName:      ltptestVolume,
		Region:          clusterName,
		AccessKey:       "39bEF4RrAQgMj6RV",
		SecretAccessKey: "TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd",
	}
	w.updateExternalS3Client(bucketInfo)
	assert.NotNilf(t, w.externalS3Client, "init s3 client")
	assert.Equalf(t, bucketInfo, w.externalS3Config, "update bucket info config")
	assert.Equalf(t, bucketInfo.BucketName, w.externalS3BucketName.ToString(), "update bucket name")

	// construct s3 file for extent key
	inode := uint64(2)
	partitionID := uint64(10)
	extentID := uint64(99)
	extOff := uint64(100)
	extSize := uint32(4096)
	extFileOffset:= uint64(200)
	s3path := proto.GenS3Key(clusterName, ltptestVolume, volID, inode, partitionID, extentID)
	s3Data := randTestData(int(extOff)+int(extSize))
	s3Filepath := path.Join("/cfs/mnt", s3path)
	err := os.MkdirAll(path.Dir(s3Filepath), 0777)
	assert.NoErrorf(t, err, "mkdir: (%v)", path.Dir(s3Filepath))
	err = ioutil.WriteFile(s3Filepath, s3Data, 0666)
	assert.NoErrorf(t, err, "write file: (%v)", s3Filepath)

	// read from s3 by extent key
	s := &Streamer{inode: inode, client: &ExtentClient{dataWrapper: w}}
	s.extents = NewExtentCache(inode)
	s.extents.insert(&proto.ExtentKey{
		FileOffset:   extFileOffset,
		PartitionId:  partitionID,
		ExtentId:     extentID,
		ExtentOffset: extOff,
		Size:         extSize,
		CRC:          uint32(proto.S3Extent),
	}, true)
	readOffset := uint64(300)
	readSize := 3996
	readData := make([]byte, readSize)
	var (
		actualRead	int
		actualHole	bool
	)
	actualRead, actualHole, err = s.read(context.Background(), readData, readOffset, readSize)
	assert.NoErrorf(t, err, "read from s3")
	assert.Equalf(t, readSize, actualRead, "read size from s3")
	assert.Falsef(t, actualHole, "read no hole")
	expectData := s3Data[extOff+readOffset-extFileOffset:]
	incorrectBegin, incorrectEnd, isSame := isSameData(expectData, readData, readSize)
	assert.Truef(t, isSame, "expectDataSize(%v) actualDataSize(%v) incorrect(%v~%v)", len(expectData), len(readData), incorrectBegin, incorrectEnd)
}