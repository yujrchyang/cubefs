package migration

import (
	"github.com/cubefs/cubefs/sdk/master"
	"sync"
)

type ClusterInfo struct {
	Name         string
	MasterClient *master.MasterClient
	VolumeMap    sync.Map // volume_info.go Vol *VolumeInfo
}

func NewClusterInfo(clusterName string, mc *master.MasterClient) (clusterInfo *ClusterInfo) {
	clusterInfo = &ClusterInfo{
		Name:         clusterName,
		MasterClient: mc,
	}
	return
}

func (cluster *ClusterInfo) StoreVol(volName string, vol *VolumeInfo) {
	cluster.VolumeMap.Store(volName, vol)
}

func (cluster *ClusterInfo) GetVolByName(name string) (vol interface{}, ok bool) {
	vol, ok = cluster.VolumeMap.Load(name)
	return
}

func (cluster *ClusterInfo) Nodes() (nodes []string) {
	return cluster.MasterClient.Nodes()
}

func (cluster *ClusterInfo) AddNode(node string) {
	cluster.MasterClient.AddNode(node)
}

func (cluster *ClusterInfo) UpdateNodes(nodes []string) {
	cluster.MasterClient = master.NewMasterClient(nodes, false)
}

func (cluster *ClusterInfo) GetClusterName() string {
	return cluster.Name
}

func (cluster *ClusterInfo) ReleaseUnusedVolume() {
	cluster.VolumeMap.Range(func(key, value interface{}) bool {
		volName := key.(string)
		if value.(*VolumeInfo).ReleaseResourceMeetCondition() {
			cluster.VolumeMap.Delete(volName)
		}
		return true
	})
}

func (cluster *ClusterInfo) UpdateVolState(volumeName string, state uint32) bool {
	var (
		vol interface{}
		ok bool
	)
	if vol, ok = cluster.VolumeMap.Load(volumeName); !ok {
		return false
	}
	vol.(*VolumeInfo).UpdateState(state)
	return true
}
