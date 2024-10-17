package metanode

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/testutil"
	"github.com/stretchr/testify/assert"
)

var (
	recorderMetaData = []byte(`
{
	"VolName":"recorder_vol",
	"PartitionID":111,
	"Peers":[
		{"id":3,"addr":"127.0.0.1:8081","type":2},
		{"id":4,"addr":"127.0.0.1:8082","type":0},
		{"id":12,"addr":"127.0.0.1:8083","type":0},
		{"id":21,"addr":"127.0.0.1:8084","type":0},
		{"id":23,"addr":"127.0.0.1:8085","type":2}
	],
	"Learners":[],
	"recorders":["127.0.0.1:8081","127.0.0.1:8085"],
	"CreateTime":"2024-05-30 11:38:36"
}
`)
)

func TestMetaLoadRecorder(t *testing.T)  {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()

	metadataDir := path.Join(testPath.Path(), "meta")
	raftDir := path.Join(testPath.Path(), "raft")
	os.MkdirAll(metadataDir, os.ModePerm)
	os.MkdirAll(raftDir, os.ModePerm)

	// create directory
	recorderPath := path.Join(metadataDir, "recorder_111")
	os.MkdirAll(recorderPath, os.ModePerm)
	err := ioutil.WriteFile(path.Join(recorderPath, raftstore.MetadataFileName), recorderMetaData, os.ModePerm)
	assert.NoErrorf(t, err, "write recorder meta file %v failed: %v", recorderPath, err)
	err = ioutil.WriteFile(path.Join(recorderPath, raftstore.ApplyIndexFileName), []byte(fmt.Sprintf("%d", 0)), os.ModePerm)
	assert.NoErrorf(t, err, "write recorder apply file %v failed: %v", recorderPath, err)

	recorderRaftPath := path.Join(raftDir, "111")
	err = os.MkdirAll(recorderRaftPath, os.ModePerm)
	assert.NoErrorf(t, err, "Make recorder raft path %v failed: %v", recorderRaftPath, err)

	// load
	manager := &metadataManager{
		nodeId:  1,
		rootDir: metadataDir,
	}
	var mr *metaRecorder
	mr, err = manager.loadRecorder("recorder_111")
	// check
	assert.NoErrorf(t, err, "load recorder failed")
	assert.Equal(t, uint64(111), mr.partitionID, "load recorder")
}