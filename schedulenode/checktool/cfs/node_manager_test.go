package cfs

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckNodeSet(t *testing.T) {
	tv := &TopologyView{
		Zones: []*ZoneView{
			{
				Name: "zone1",
				NodeSet: map[uint64]*nodeSetView{
					1: {
						DataNodeLen: 1,
						MetaNodeLen: 20,
					},
					2: {
						DataNodeLen: 20,
						MetaNodeLen: 25,
					},
				},
			},
			{
				Name: "zone1",
				NodeSet: map[uint64]*nodeSetView{
					1: {
						DataNodeLen: 0,
						MetaNodeLen: 0,
					},
					2: {
						DataNodeLen: 200,
						MetaNodeLen: 200,
					},
				},
			},
			{
				Name: "zone1",
				NodeSet: map[uint64]*nodeSetView{
					1: {
						DataNodeLen: 200,
						MetaNodeLen: 200,
					},
					2: {
						DataNodeLen: 231,
						MetaNodeLen: 233,
					},
				},
			},
		},
	}
	badDataSets, badMetaSets := checkNodeSetLen(tv, "test.cluster")
	assert.Equal(t, 2, len(badDataSets))
	assert.Equal(t, 1, len(badMetaSets))
}
