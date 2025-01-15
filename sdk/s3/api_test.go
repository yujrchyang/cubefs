package s3

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

const (
	region     = "chubaofs01"
	endPoint   = "http://object.chubao.io"
	accessKey  = "39bEF4RrAQgMj6RV"
	secretKey  = "TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd"
	bucketName = "ltptest"
	keyPrefix  = "test-S3-GO-API"
)

var (
	fileDataLen = 32*unit.MB
	fileData = make([]byte, fileDataLen)
	ctx = context.Background()
	s3Client = NewS3Client(region, endPoint, accessKey, secretKey, false)
)

func init() {
	//gen data
	rand.Seed(time.Now().UnixNano())
	offset := 0
	for {
		if offset == len(fileData) {
			break
		}
		randomValue := rand.Uint64()
		binary.BigEndian.PutUint64(fileData[offset:offset+8], randomValue)
		offset += 8
	}
}

func TestS3API_HeadBucket(t *testing.T) {
	exist, err := s3Client.CheckBucketExist(ctx, bucketName)
	assert.Nil(t, err)
	assert.Equal(t, true, exist)

	exist, err = s3Client.CheckBucketExist(ctx, bucketName+ "NotExist")
	assert.Nil(t, err)
	assert.Equal(t, false, exist)
}

func TestS3API_PutAndGetObject(t *testing.T) {
	key := keyPrefix + "/file1"
	err := s3Client.PutObject(ctx, bucketName, key, fileData)
	assert.Nil(t, err, fmt.Sprintf("error expect nil, but: %v", err))
	fmt.Printf("put object %s finised\n", key)

	var exist bool
	exist, err = s3Client.HeadObject(ctx, bucketName, key)
	assert.Nil(t, err)
	assert.Equal(t, exist, true)

	defer func() {
		_ = s3Client.DeleteObject(ctx, bucketName, key)
		fmt.Printf("delete object %s finised\n", key)
		exist, err = s3Client.HeadObject(ctx, bucketName, key)
		assert.Nil(t, err)
		assert.Equal(t, exist, false)
	}()

	var (
		readN       int
		offset      uint32
		readDataLen = 4*unit.MB
		readData    = make([]byte, readDataLen)
	)

	for {
		if offset == uint32(len(fileData)) {
			break
		}
		readN, _ = s3Client.GetObject(ctx, bucketName, key, uint64(offset), uint64(readDataLen), readData)
		assert.Nil(t, err, fmt.Sprintf("error expect nil, but: %v", err))
		assert.Equal(t, readDataLen, readN, fmt.Sprintf("read from %v to %v with unexpect read count", offset, offset+uint32(readDataLen)))
		assert.Equal(t, fileData[int(offset):int(offset)+readDataLen], readData)
		offset += uint32(readDataLen)
	}
	fmt.Printf("get object %s finished\n", key)
}

func TestS3API_BatchDeleteObjects(t *testing.T) {
	var keys = make([]string, 0, 100)
	var exist bool
	var key string
	var err error
	for index := 0; index < 100; index++ {
		key = keyPrefix + "/file" + strconv.Itoa(index)
		err = s3Client.PutObject(ctx, bucketName, key, fileData)
		assert.Nil(t, err, fmt.Sprintf("error expect nil, but: %v", err))

		exist, err = s3Client.HeadObject(ctx, bucketName, key)
		assert.Nil(t, err)
		assert.Equal(t, exist, true, fmt.Sprintf("key %s expect exist", key))

		keys = append(keys, key)
	}

	_, _ = s3Client.BatchDeleteObject(ctx, bucketName, keys)
	fmt.Printf("batch delete objects %s finised\n", keys)
	for _, key = range keys {
		exist, err = s3Client.HeadObject(ctx, bucketName, key)
		assert.Nil(t, err)
		assert.Equal(t, exist, false, fmt.Sprintf("key %s expect not exist", key))
	}
}

func TestS3Client_UploadFileByPart(t *testing.T) {
	key := keyPrefix + "/file2"
	var partCount = 8
	var perPartSize = 4*unit.MB
	err := s3Client.UploadByPart(ctx, bucketName, key, partCount, partCount, func(index int) (data []byte, err error) {
		offset := index*perPartSize
		return fileData[offset:offset+perPartSize], nil
	})
	assert.Nil(t, err, fmt.Sprintf("error expect nil, but: %v", err))

	defer func() {
		_ = s3Client.DeleteObject(ctx, bucketName, key)
		fmt.Printf("delete object %s finised\n", key)
	}()

	var (
		readN       int
		offset      uint32
		readDataLen = 4*unit.MB
		readData    = make([]byte, readDataLen)
	)

	for {
		if offset == uint32(len(fileData)) {
			break
		}
		readN, _ = s3Client.GetObject(ctx, bucketName, key, uint64(offset), uint64(readDataLen), readData)
		assert.Nil(t, err, fmt.Sprintf("error expect nil, but: %v", err))
		assert.Equal(t, readDataLen, readN, fmt.Sprintf("read from %v to %v with unexpect read count", offset, offset+uint32(readDataLen)))
		assert.Equal(t, fileData[int(offset):int(offset)+readDataLen], readData)
		offset += uint32(readDataLen)
	}
	fmt.Printf("get object %s finished\n", key)
}

func TestS3Client_DeleteObjectsByPrefix(t *testing.T) {
	var key string
	var err error
	var actualCount int
	var expectCount int
	rr := rand.New(rand.NewSource(time.Now().UnixNano()))
	for index := 1; index < 100; index++ {
		fileCount := rr.Int31n(10)
		if fileCount <= 1 {
			continue
		}
		for fileID := int32(1); fileID < fileCount; fileID ++ {
			key = fmt.Sprintf("%s/%v/%v", keyPrefix, index, fileID)
			err = s3Client.PutObject(ctx, bucketName, key, fileData)
			assert.Nil(t, err, fmt.Sprintf("error expect nil, but: %v", err))
		}
		expectCount += int(fileCount)
	}

	actualCount, err = s3Client.GetObjectsCountByPrefix(ctx, bucketName, keyPrefix)
	assert.Nil(t, err, fmt.Sprintf("error expect nil, but: %v", err))
	assert.Equal(t, expectCount, actualCount, fmt.Sprintf("object with prefix %s expect count: %v, actual: %v",  keyPrefix, expectCount, actualCount))

	err = s3Client.DeleteObjectsByPrefix(ctx, bucketName, keyPrefix)
	assert.Nil(t, err, fmt.Sprintf("error expect nil, but: %v", err))
	actualCount, err = s3Client.GetObjectsCountByPrefix(ctx, bucketName, keyPrefix)
	assert.Equal(t, 0, actualCount, fmt.Sprintf("object with prefix %s expect count: %v, actual: %v", keyPrefix, 0, actualCount))
}