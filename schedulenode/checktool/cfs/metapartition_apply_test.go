package cfs

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/tcp_api"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func TestMetaPartitionApply(t *testing.T) {
	//t.Skipf("skip")
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	t.Run("dbbak", func(t *testing.T) {
		host := newClusterHost("", true)
		checkMetaPartitionApply(host)
	})
	t.Run("spark", func(t *testing.T) {
		host := newClusterHost("", false)
		checkMetaPartitionApply(host)
	})
}

func TestCompareMetaLoadInfo(t *testing.T) {
	metaInfos := make(map[string]*tcp_api.MetaPartitionLoadResponse, 0)
	metaInfos["node1"] = &tcp_api.MetaPartitionLoadResponse{
		PartitionID: 1,
		ApplyID:     1000,
	}
	metaInfos["node2"] = &tcp_api.MetaPartitionLoadResponse{
		PartitionID: 1,
		ApplyID:     10000,
	}
	metaInfos["node3"] = &tcp_api.MetaPartitionLoadResponse{
		PartitionID: 1,
		ApplyID:     1000,
	}
	var minReplica *tcp_api.MetaPartitionLoadResponse
	minReplica, same := compareLoadResponse(200, 0, func(mpr *tcp_api.MetaPartitionLoadResponse) uint64 { return mpr.ApplyID }, metaInfos)
	assert.False(t, same)
	assert.Equal(t, uint64(1000), minReplica.ApplyID)

	metaInfos["node2"] = &tcp_api.MetaPartitionLoadResponse{
		PartitionID: 1,
		ApplyID:     1000,
	}

	minReplica, same = compareLoadResponse(200, 0, func(mpr *tcp_api.MetaPartitionLoadResponse) uint64 { return mpr.ApplyID }, metaInfos)
	assert.True(t, same)
}

func TestServerStartedCheck(t *testing.T) {
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	started := isServerStartCompleted("")
	fmt.Printf("%v", started)
}

func TestServerStatCheck(t *testing.T) {
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	addr := ""
	if addr == "" {
		return
	}

	started := isServerAlreadyStart(addr, time.Minute*2)
	fmt.Printf("%v\n", started)
}

func TestUploadMetaNodeStack(t *testing.T) {
	addr := ""
	if addr == "" {
		return
	}

	httpClient := &http.Client{
		Timeout: time.Minute * 2,
	}
	s3Client = s3.NewFromConfig(aws.Config{
		HTTPClient: httpClient,
	},
		func(o *s3.Options) {
			o.BaseEndpoint = aws.String("")
		},
		func(o *s3.Options) {
			o.Credentials = credentials.NewStaticCredentialsProvider("", "", "")
		},
		func(o *s3.Options) {
			o.Region = ""
		},
	)
	bucketName = ""
	uploadMetaNodeStack(addr)
}
