package file

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/console/model"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	s3 "github.com/cubefs/cubefs/objectnode"
	"github.com/cubefs/cubefs/sdk/master"
)

type ObjectManager struct {
	endPoint   map[string]string
	volManager map[string]*s3.VolumeManager // clusterName - vol manager
	api        *api.APIManager
	sync.RWMutex
}

func NewObjectManager(clusters []*model.ConsoleCluster) *ObjectManager {
	objectManager := new(ObjectManager)
	objectManager.api = api.NewAPIManager(clusters)
	objectManager.volManager = make(map[string]*s3.VolumeManager)
	objectManager.endPoint = make(map[string]string)
	for _, cluster := range clusters {
		addrs := strings.Split(cluster.MasterAddrs, ",")
		objectManager.volManager[cluster.ClusterName] = s3.NewVolumeManager(addrs, true)
		objectManager.endPoint[cluster.ClusterName] = cluster.ObjectDomain
	}
	return objectManager
}

func (o *ObjectManager) GetUserClient(cluster string) *master.MasterClient {
	return o.api.GetMasterClient(cluster)
}

func (o *ObjectManager) GetS3EndPoint(cluster string) (string, error) {
	if cluster != "spark" {
		return "", fmt.Errorf("%s集群不支持S3", cluster)
	}
	o.RLock()
	defer o.RUnlock()

	endPoint := o.endPoint[cluster]
	return endPoint, nil
}

func (o *ObjectManager) GetVolManager(cluster string) (*s3.VolumeManager, error) {
	if cluster != "spark" {
		return nil, fmt.Errorf("%s集群不支持S3", cluster)
	}
	o.RLock()
	defer o.RUnlock()

	manager := o.volManager[cluster]
	return manager, nil
}
