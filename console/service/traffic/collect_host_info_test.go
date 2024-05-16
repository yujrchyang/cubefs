package traffic

import (
	"github.com/cubefs/cubefs/console/model"
	"testing"
)

func Test_handleNodeUsageRatio(t *testing.T) {
	initApiSdk()

	recordCh := make(chan []*model.ClusterHostInfo, 10)
	go recordHostInfo(recordCh)
	handleNodeUsageRatio("spark", recordCh)
}
