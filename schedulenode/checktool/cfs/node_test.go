package cfs

import "testing"

func TestNodeRiskCheck(t *testing.T) {
	t.Skipf("This case skipped for unavailable cluster.")
	host := &ClusterHost{
		host:             "10.179.20.34:80",
		isReleaseCluster: false,
	}
	checkDataNodeRiskData(host)
}
