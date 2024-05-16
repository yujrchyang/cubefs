package apiManager

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
)

var (
	Global_CacheManager *cacheManager
)

const (
	maxViewCacheDuration = time.Duration(5 * time.Minute)
	volListCacheDuration = time.Duration(10 * time.Minute)
)

type CacheManager interface {
	GetCache(cluster string, force bool) (interface{}, error)
	UpdateCache(cluster string) error
}

type cacheManager struct {
	api                *APIManager
	clusterViewCache   *ClusterViewCache
	limitInfoCache     *LimitInfoCache
	flashNodeViewCache *FlashNodeViewCache
	volListCache       *VolumeListCache
}

func initCacheManager(api *APIManager) *cacheManager {
	if Global_CacheManager == nil {
		Global_CacheManager = newCacheManager(api)
		log.LogInfof("InitCacheManager success: apiManager(%v)", api)
	}
	return Global_CacheManager
}

func newCacheManager(api *APIManager) *cacheManager {
	return &cacheManager{
		api:                api,
		clusterViewCache:   new(ClusterViewCache),
		limitInfoCache:     new(LimitInfoCache),
		flashNodeViewCache: new(FlashNodeViewCache),
		volListCache:       new(VolumeListCache),
	}
}

func (cache *cacheManager) GetClusterViewCache(cluster string, update bool) (interface{}, error) {
	value, err := cache.clusterViewCache.GetCache(cluster, update)
	// log.LogDebugf("GetClusterViewCache: cluster(%v) update(%v) value(%v), err(%v)", cluster, update, value, err)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (cache *cacheManager) UpdateClusterViewCache(cluster string) error {
	return cache.clusterViewCache.UpdateCache(cluster)
}

func (cache *cacheManager) GetLimitInfoCache(cluster string, update bool) (interface{}, error) {
	value, err := cache.limitInfoCache.GetCache(cluster, update)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (cache *cacheManager) UpdateLimitInfoCache(cluster string) error {
	return cache.limitInfoCache.UpdateCache(cluster)
}

func (cache *cacheManager) GetFlashNodeViewCache(cluster string, force bool) ([]*cproto.ClusterFlashNodeView, error) {

	value, err := cache.flashNodeViewCache.GetCache(cluster, force)
	if err != nil {
		return nil, err
	}

	if data, ok := value.([]*cproto.ClusterFlashNodeView); ok {
		return data, nil
	}

	return nil, nil
}

func (cache *cacheManager) UpdateFlashNodeViewCache(cluster string) error {
	return cache.flashNodeViewCache.UpdateCache(cluster)
}

type ClusterViewCache struct {
	sync.RWMutex
	cvCache map[string]*clusterViewCacheEntity
}

type clusterViewCacheEntity struct {
	view        interface{} // proto.ClusterView or cproto.ClusterView
	cacheExpire time.Time
}

func (c *ClusterViewCache) GetCache(cluster string, forceUpdate bool) (view interface{}, err error) {

	if c.cvCache == nil || forceUpdate {
		if err = c.UpdateCache(cluster); err != nil {
			return
		}
	}
	c.RLock()
	if cacheEntity, ok := c.cvCache[cluster]; ok && cacheEntity.cacheExpire.After(time.Now()) {
		view = cacheEntity.view
		c.RUnlock()
		return
	}
	c.RUnlock()

	if err = c.UpdateCache(cluster); err == nil {
		c.RLock()
		view = c.cvCache[cluster].view
		c.RUnlock()
	}
	return
}

func (c *ClusterViewCache) UpdateCache(cluster string) error {
	newCache := new(clusterViewCacheEntity)
	if cproto.IsRelease(cluster) {
		client := Global_CacheManager.api.GetReleaseClient(cluster)
		cv, err := client.GetClusterView()
		if err != nil {
			log.LogWarnf("UpdateCache: get clusterView failed, cluster[%v] err: %v", cluster, err)
			return err
		}
		newCache.view = cv
	} else {
		cv, err := Global_CacheManager.api.AdminGetCluster(cluster)
		// protobuf编码导致数据拿不到,用单独封装的接口
		if err != nil {
			log.LogWarnf("UpdateCache: get clusterView failed, cluster[%v] err: %v", cluster, err)
			return err
		}
		newCache.view = cv
	}

	log.LogInfof("UpdateCache clusterView success, cluster(%v)", cluster)

	newCache.cacheExpire = time.Now().Add(maxViewCacheDuration)

	c.Lock()
	defer c.Unlock()

	cache := c.cvCache
	if cache == nil {
		cache = make(map[string]*clusterViewCacheEntity)
	}
	cache[cluster] = newCache
	c.cvCache = cache
	return nil
}

type LimitInfoCache struct {
	sync.RWMutex
	limitCache map[string]*limitInfoCacheEntity
}

type limitInfoCacheEntity struct {
	info        interface{} //*proto.LimitInfo or *cproto.LimitInfo
	cacheExpire time.Time
}

func (l *LimitInfoCache) GetCache(cluster string, forceUpdate bool) (info interface{}, err error) {
	if l.limitCache == nil || forceUpdate {
		if err = l.UpdateCache(cluster); err != nil {
			return
		}
	}
	l.RLock()
	if cacheEntity, ok := l.limitCache[cluster]; ok && cacheEntity.cacheExpire.After(time.Now()) {
		info = cacheEntity.info
		l.RUnlock()
		return
	}
	l.RUnlock()

	if err = l.UpdateCache(cluster); err == nil {
		l.RLock()
		info = l.limitCache[cluster].info
		l.RUnlock()
	}
	return
}

func (l *LimitInfoCache) UpdateCache(cluster string) error {
	newCache := new(limitInfoCacheEntity)
	if cproto.IsRelease(cluster) {
		client := Global_CacheManager.api.GetReleaseClient(cluster)
		info, err := client.GetLimitInfo("")
		if err != nil {
			log.LogWarnf("UpdateCache: get limitInfo failed, cluster[%v] err: %v", cluster, err)
			return err
		}
		newCache.info = info
	} else {
		client := Global_CacheManager.api.GetMasterClient(cluster)
		info, err := client.AdminAPI().GetLimitInfo("")
		if err != nil {
			log.LogWarnf("UpdateCacheL: get limitInfo failed, cluster[%v] err: %v", cluster, err)
			return err
		}
		newCache.info = info
	}

	log.LogInfof("UpdateCache limitInfo success, cluster(%v)", cluster)

	newCache.cacheExpire = time.Now().Add(maxViewCacheDuration)

	l.Lock()
	defer l.Unlock()

	cache := l.limitCache
	if cache == nil {
		cache = make(map[string]*limitInfoCacheEntity)
	}
	cache[cluster] = newCache
	l.limitCache = cache
	return nil
}

type FlashNodeViewCache struct {
	sync.RWMutex
	cache map[string]*flashNodeCacheEntity
}

type flashNodeCacheEntity struct {
	data        []*cproto.ClusterFlashNodeView
	expiredTime time.Time
}

func (c *FlashNodeViewCache) GetCache(cluster string, forceUpdate bool) (data interface{}, err error) {

	if c.cache == nil || forceUpdate {
		err = c.UpdateCache(cluster)
		if err != nil {
			return nil, err
		}
	}

	c.RLock()
	if cacheEntity, ok := c.cache[cluster]; ok && cacheEntity.expiredTime.After(time.Now()) {
		data = cacheEntity.data
		c.RUnlock()
		return data, nil
	}
	c.RUnlock()
	if err = c.UpdateCache(cluster); err == nil {
		c.RLock()
		data = c.cache[cluster].data
		c.RUnlock()
	}
	return
}

func (c *FlashNodeViewCache) UpdateCache(cluster string) error {
	client := Global_CacheManager.api.GetMasterClient(cluster)
	data, err := getFlashNodeView(client)
	if err != nil {
		log.LogWarnf("UpdateCache: get flashNodeListView failed, err: %v", err)
		return err
	}
	newCacheEntity := &flashNodeCacheEntity{
		data:        data,
		expiredTime: time.Now().Add(maxViewCacheDuration),
	}

	c.Lock()
	defer c.Unlock()

	cache := c.cache
	if cache == nil {
		cache = make(map[string]*flashNodeCacheEntity)
	}
	cache[cluster] = newCacheEntity
	c.cache = cache
	return nil
}

func getFlashNodeView(masterClient *master.MasterClient) ([]*cproto.ClusterFlashNodeView, error) {
	zoneFlashNodes, err := masterClient.AdminAPI().GetAllFlashNodes(true)
	if err != nil {
		return nil, err
	}
	var (
		flashViews = make([]*cproto.ClusterFlashNodeView, 0)
	)
	for _, flashNodeViewInfos := range zoneFlashNodes {
		sort.Slice(flashNodeViewInfos, func(i, j int) bool {
			return flashNodeViewInfos[i].ID < flashNodeViewInfos[j].ID
		})

		for _, fn := range flashNodeViewInfos {
			var (
				hitRate = "N/A"
				evicts  = "N/A"
				limit   = "N/A"
			)
			if fn.IsActive && fn.IsEnable {
				fnClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(fn.Addr, ":")[0], masterClient.FlashNodeProfPort), false)
				stat, err1 := fnClient.GetStat()
				if err1 == nil {
					hitRate = fmt.Sprintf("%.2f%%", stat.CacheStatus.HitRate*100)
					evicts = strconv.Itoa(stat.CacheStatus.Evicts)
					limit = strconv.FormatUint(stat.NodeLimit, 10)
				}
			}
			view := newClusterFlashNodeView(fn, hitRate, evicts, limit)
			flashViews = append(flashViews, view)
		}
	}
	return flashViews, nil
}

func newClusterFlashNodeView(info *proto.FlashNodeViewInfo, hitRate, evicts, limit string) *cproto.ClusterFlashNodeView {
	return &cproto.ClusterFlashNodeView{
		ID:           info.ID,
		Addr:         info.Addr,
		ReportTime:   info.ReportTime.Format("2006-01-02 15:04:05"),
		IsActive:     info.IsActive,
		ZoneName:     info.ZoneName,
		FlashGroupID: info.FlashGroupID,
		IsEnable:     info.IsEnable,
		NodeLimit:    limit,
		HitRate:      hitRate,
		Evicts:       evicts,
	}
}

type VolumeListCache struct {
	sync.RWMutex
	cache map[string]*volumeCacheEntity
}

type volumeCacheEntity struct {
	data        []*proto.VolInfo
	expiredTime time.Time
}

func (v *VolumeListCache) GetCache(cluster string, forceUpdate bool) (data []*proto.VolInfo, err error) {

	if v.cache == nil || forceUpdate {
		err = v.UpdateCache(cluster)
		if err != nil {
			return nil, err
		}
	}
	v.RLock()
	if cacheEntity, ok := v.cache[cluster]; ok && cacheEntity.expiredTime.After(time.Now()) {
		data = cacheEntity.data
		v.RUnlock()
		return
	}
	v.RUnlock()

	if err = v.UpdateCache(cluster); err == nil {
		v.RLock()
		data = v.cache[cluster].data
		v.RUnlock()
	}
	return
}

func (v *VolumeListCache) UpdateCache(cluster string) error {
	newCache := new(volumeCacheEntity)

	mc := Global_CacheManager.api.GetMasterClient(cluster)
	volsInfo, err := mc.AdminAPI().ListVols("")
	if err != nil {
		log.LogWarnf("UpdateCache: get ListVols failed, cluster(%v) err(%v)", cluster, err)
		return err
	}
	newCache.data = volsInfo

	newCache.expiredTime = time.Now().Add(volListCacheDuration)

	v.Lock()
	defer v.Unlock()

	cache := v.cache
	if cache == nil {
		cache = make(map[string]*volumeCacheEntity)
	}
	cache[cluster] = newCache
	v.cache = cache
	return nil
}

func (cache *cacheManager) GetVolListCache(cluster string, force bool) ([]string, error) {
	volList, err := cache.volListCache.GetCache(cluster, force)
	if err != nil {
		return nil, err
	}
	volNames := make([]string, 0, len(volList))
	for _, vol := range volList {
		volNames = append(volNames, vol.Name)
	}
	return volNames, nil
}

func (cache *cacheManager) ManualAddVolToCache(cluster, vol string) {
	cache.volListCache.RLock()
	cacheEntity, ok := cache.volListCache.cache[cluster]
	cache.volListCache.RUnlock()
	if !ok {
		return
	}
	newVol := &proto.VolInfo{
		Name: vol,
	}
	cache.volListCache.Lock()
	cacheEntity.data = append(cacheEntity.data, newVol)
	cache.volListCache.Unlock()
	return
}
