package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	cluster_service "github.com/cubefs/cubefs/console/service/cluster"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

const (
	parallelThreadForGetVol int = 10
)

type VolumeService struct {
	api *api.APIManager
	vs  *VolumeClientService
}

func NewVolumeService(clusters []*model.ConsoleCluster) *VolumeService {
	cliService := api.NewAPIManager(clusters)
	return &VolumeService{
		api: cliService,
		vs:  NewVolumeVersionService(&cutil.Global_CFG),
	}
}

func (v *VolumeService) SType() cproto.ServiceType {
	return cproto.VolumeService
}

func (v *VolumeService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	v.registerObject(schema)
	v.registerQuery(schema)
	v.registerMutation(schema)

	return schema.MustBuild()
}

func (v *VolumeService) registerObject(schema *schemabuilder.Schema) {

}

func (v *VolumeService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()
	query.FieldFunc("volList", v.getVolList)
	query.FieldFunc("getVolume", v.getVolume) //操作中展示属性的接口
	query.FieldFunc("listVolume", v.listVolume)
	query.FieldFunc("volDataPartitions", v.volDataPartitions)
	query.FieldFunc("volMetaPartitions", v.volMetaPartitions)
	query.FieldFunc("volPermission", v.volPermission)
	query.FieldFunc("getKeys", v.getKeys)
	query.FieldFunc("getClientListUrl", v.getClientListUrl)
}

func (v *VolumeService) registerMutation(schema *schemabuilder.Schema) {
	mutation := schema.Mutation()

	mutation.FieldFunc("createVolume", v.createVolume)
	mutation.FieldFunc("updateVolume", v.updateVolume) // vol编辑页(不全，只有一部分)
}

func (v *VolumeService) DoXbpApply(apply *model.XbpApplyInfo) error {
	return nil
}

func (v *VolumeService) volPermission(ctx context.Context, args struct {
	Cluster    *string
	Name       string
	UserID     *string // 授权时填写
	Permission *string
}) ([]*cproto.UserPermission, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	if cproto.IsRelease(cluster) {
		return nil, cproto.ErrUnSupportOperation
	}
	users, err := v.api.ListUsersOfVol(cluster, args.Name)
	if err != nil {
		return nil, err
	}
	mc := v.api.GetMasterClient(cluster)
	if args.UserID != nil && args.Permission != nil {
		var exist = func(l []string, t string) bool {
			for _, k := range l {
				if k == t {
					return true
				}
			}
			return false
		}
		if exist(users, *args.UserID) {
			return nil, nil
		}
		param := &proto.UserPermUpdateParam{
			UserID: *args.UserID,
			Volume: args.Name,
			Policy: []string{*args.Permission},
		}
		_, err = mc.UserAPI().UpdatePolicy(param)
		if err != nil {
			return nil, err
		}
		users = append(users, *args.UserID)
	}

	userPList := make([]*cproto.UserPermission, 0, len(users))
	for _, user := range users {
		info, err := mc.UserAPI().GetUserInfo(user)
		if err != nil {
			continue
		}
		userPList = append(userPList, &cproto.UserPermission{
			UserID: user,
			// 	拥有的卷 access为空
			Access:  strings.Join(info.Policy.AuthorizedVols[args.Name], ","),
			IsOwner: info.Policy.IsOwn(args.Name),
		})
	}
	return userPList, nil
}

func (v *VolumeService) getKeys(ctx context.Context, args struct {
	Cluster *string
	Name    string
	UserID  string
}) (*proto.UserInfo, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	if cproto.IsRelease(cluster) {
		return nil, cproto.ErrUnSupportOperation
	}
	return v.api.GetMasterClient(cluster).UserAPI().GetUserInfo(args.UserID)
}

func (v *VolumeService) getClientListUrl(ctx context.Context, args struct {
	Cluster string
	Volume  string
}) (string, error) {

	return v.vs.GetVolumeClientListUrl(args.Cluster, args.Volume)
}

func (v *VolumeService) getVolume(ctx context.Context, args struct {
	Cluster *string
	Name    string
}) (*cproto.Vol, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	return v.getVolumeView(cluster, args.Name)
}

func (v *VolumeService) volDataPartitions(ctx context.Context, args struct {
	Cluster  string
	Volume   string
	Page     int32
	PageSize int32
}) (*cproto.PartitionListResponse, error) {
	cluster := args.Cluster
	vol := args.Volume
	resp := new(cproto.PartitionListResponse)

	if cproto.IsRelease(cluster) {
		rc := v.api.GetReleaseClient(cluster)
		dps, err := rc.ClientDataPartitions(vol)
		if err != nil {
			return nil, err
		}
		resp.Total = len(dps)
		resp.Data = getVolDataPartitions(cluster, dps, int(args.Page), int(args.PageSize))
	} else {
		mc := v.api.GetMasterClient(cluster)
		view, err := mc.ClientAPI().GetDataPartitions(vol, nil)
		if err != nil {
			return nil, err
		}
		resp.Total = len(view.DataPartitions)
		resp.Data = getVolDataPartitions(cluster, view.DataPartitions, int(args.Page), int(args.PageSize))
	}
	return resp, nil
}

// 按id大小排序
func getVolDataPartitions(cluster string, data interface{}, page, pageSize int) (result []*cproto.PartitionViewDetail) {
	result = make([]*cproto.PartitionViewDetail, 0)
	if cproto.IsRelease(cluster) {
		dps := data.([]*cproto.DataPartitionsView)
		if len(dps) > 0 {
			sort.SliceStable(dps, func(i, j int) bool {
				return dps[i].PartitionID < dps[j].PartitionID
			})
			dps = dps[(page-1)*pageSize : unit.Min(page*pageSize, len(dps))]
		}
		for _, dp := range dps {
			view := &cproto.PartitionViewDetail{
				PartitionID: dp.PartitionID,
				Status:      formatPartitionStatus(dp.Status),
				Hosts:       cluster_service.FormatHost(dp.Hosts, make([]string, len(dp.Hosts))),
				ReplicaNum:  int(dp.ReplicaNum),
			}
			result = append(result, view)
		}
	} else {
		dps := data.([]*proto.DataPartitionResponse)
		if len(dps) > 0 {
			sort.SliceStable(dps, func(i, j int) bool {
				return dps[i].PartitionID < dps[j].PartitionID
			})
			dps = dps[(page-1)*pageSize : unit.Min(page*pageSize, len(dps))]
		}
		for _, dp := range dps {
			view := &cproto.PartitionViewDetail{
				PartitionID:     dp.PartitionID,
				ReplicaNum:      int(dp.ReplicaNum),
				Status:          formatPartitionStatus(dp.Status),
				EcMigrateStatus: cluster_service.EcStatusMap[dp.EcMigrateStatus],
				IsRecover:       dp.IsRecover,
				Leader:          dp.LeaderAddr.ToString(),
				Hosts:           cluster_service.FormatHost(dp.Hosts, make([]string, len(dp.Hosts))),
			}
			result = append(result, view)
		}
	}
	return result
}

func (v *VolumeService) volMetaPartitions(ctx context.Context, args struct {
	Cluster  string
	Volume   string
	Page     int32
	PageSize int32
}) (*cproto.PartitionListResponse, error) {
	cluster := args.Cluster
	vol := args.Volume
	resp := new(cproto.PartitionListResponse)
	if cproto.IsRelease(cluster) {
		rc := v.api.GetReleaseClient(cluster)
		mps, err := rc.ClientMetaPartition(vol)
		if err != nil {
			return nil, err
		}
		resp.Total = len(mps)
		resp.Data = getVolMetaPartitions(cluster, mps, int(args.Page), int(args.PageSize))
	} else {
		mc := v.api.GetMasterClient(cluster)
		mps, err := mc.ClientAPI().GetMetaPartitions(vol)
		if err != nil {
			return nil, err
		}
		resp.Total = len(mps)
		resp.Data = getVolMetaPartitions(cluster, mps, int(args.Page), int(args.PageSize))
	}
	return resp, nil
}

func getVolMetaPartitions(cluster string, data interface{}, page, pageSize int) (result []*cproto.PartitionViewDetail) {
	result = make([]*cproto.PartitionViewDetail, 0)
	if cproto.IsRelease(cluster) {
		mps := data.([]*cproto.MetaPartitionView)
		if len(mps) > 0 {
			sort.SliceStable(mps, func(i, j int) bool {
				return mps[i].PartitionID < mps[j].PartitionID
			})
			mps = mps[(page-1)*pageSize : unit.Min(page*pageSize, len(mps))]
		}
		for _, mp := range mps {
			view := &cproto.PartitionViewDetail{
				PartitionID: mp.PartitionID,
				Status:      formatPartitionStatus(mp.Status),
				ReplicaNum:  len(mp.Members),
				Hosts:       cluster_service.FormatHost(mp.Members, make([]string, len(mp.Members))),
				Leader:      mp.LeaderAddr,
				Start:       mp.Start,
				End:         mp.End,
				InodeCount:  mp.InodeCount,
				DentryCount: mp.DentryCount,
				MaxExistIno: mp.MaxInodeID,
				IsRecover:   mp.IsRecover,
			}
			result = append(result, view)
		}
	} else {
		mps := data.([]*proto.MetaPartitionView)
		if len(mps) > 0 {
			sort.SliceStable(mps, func(i, j int) bool {
				return mps[i].PartitionID < mps[j].PartitionID
			})
			mps = mps[(page-1)*pageSize : unit.Min(page*pageSize, len(mps))]
		}
		for _, mp := range mps {
			view := &cproto.PartitionViewDetail{
				PartitionID: mp.PartitionID,
				Status:      formatPartitionStatus(mp.Status),
				ReplicaNum:  len(mp.Members),
				Hosts:       cluster_service.FormatHost(mp.Members, make([]string, len(mp.Members))),
				Leader:      mp.LeaderAddr,
				Start:       mp.Start,
				End:         mp.End,
				InodeCount:  mp.InodeCount,
				DentryCount: mp.DentryCount,
				MaxExistIno: mp.MaxExistIno,
				IsRecover:   mp.IsRecover,
			}
			result = append(result, view)
		}
	}
	return result
}

func (v *VolumeService) listVolume(ctx context.Context, args struct {
	Cluster  *string
	Page     int32
	PageSize int32
	Name     *string // 搜索框中的关键字
}) (*cproto.VolInfoResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	keywords := ""
	if args.Name != nil {
		keywords = *args.Name
	}
	total, vols, err := v.filterVols(cluster, keywords, int(args.Page), int(args.PageSize), v.GetVolList)
	if err != nil {
		return nil, err
	}
	resp := &cproto.VolInfoResponse{
		Total: total,
	}
	if total > 0 {
		resp.Data = v.parallelGetVol(cluster, vols, v.GetVol)
	}
	return resp, nil
}

func (v *VolumeService) getVolList(ctx context.Context, args struct {
	Cluster  *string
	Keywords *string
}) ([]string, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	var result = make([]string, 0)
	vols, err := v.api.GetVolNameList(cluster)
	if err != nil {
		log.LogErrorf("getVolList failed: cluster(%v) err(%v)", args.Cluster, err)
		return nil, err
	}
	if args.Keywords != nil {
		keywords := *args.Keywords
		for _, name := range vols {
			if strings.HasPrefix(name, keywords) {
				result = append(result, name)
			}
		}
	} else {
		result = vols
	}
	total := len(result)
	return result[0:unit.Min(cproto.MaxVolumeListBatch, total)], nil
}

func (v *VolumeService) createVolume(ctx context.Context, args struct {
	Cluster                        *string
	Name, Owner, Zone, Description string // 	做下拉
	Capacity                       int64
	DpReplicaNum                   *int32
}) (*cproto.GeneralResp, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	var (
		defaultDpReplicaNum int32 = 3
		err                 error
	)
	args.DpReplicaNum = &defaultDpReplicaNum
	if cproto.IsRelease(cluster) {
		err = v.api.GetReleaseClient(cluster).CreateVol(args.Name, args.Owner, int(*args.DpReplicaNum), int(args.Capacity))
	} else {
		err = v.api.CreateVolume(cluster, args.Name, args.Owner, args.Zone, args.Description, int(args.Capacity), int(*args.DpReplicaNum))
	}
	if err != nil {
		log.LogErrorf("createVolume failed: cluster(%v)name(%v)err(%v)", cluster, args.Name, err)
		return cproto.BuildResponse(err), err
	}
	v.updateVolListCache(cluster, args.Name, "create")
	record := model.NewVolumeOperation(
		cluster,
		args.Name,
		proto.AdminCreateVol,
		ctx.Value(cutil.PinKey).(string),
	)
	record.InsertRecord(record)
	log.LogInfof("创建vol成功: %v:%v", cluster, args.Name)
	return cproto.BuildResponse(err), err
}

func (v *VolumeService) updateVolume(ctx context.Context, args struct {
	Cluster                                                                       *string
	Name, AuthKey                                                                 string
	Capacity, DpReplicaNum, DefaultStoreMode, TrashRemainingDays                  *uint64
	EnableToken, ForceROW, EnableWriteCache, FollowerRead, RemoteCacheBoostEnable *bool
	DpSelectorName                                                                *string
}) (*cproto.Vol, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	view, err := v.getVolumeView(cluster, args.Name)
	if err != nil {
		return nil, err
	}
	if args.Capacity == nil {
		args.Capacity = &view.Capacity
	}
	if args.EnableToken == nil {
		args.EnableToken = &view.EnableToken
	}
	if args.DpReplicaNum == nil {
		t := uint64(view.DpReplicaNum)
		args.DpReplicaNum = &t
	}
	if args.DefaultStoreMode == nil {
		t := uint64(view.DefaultStoreMode)
		args.DefaultStoreMode = &t
	}
	if args.TrashRemainingDays == nil {
		t := uint64(view.TrashRemainingDays)
		args.TrashRemainingDays = &t
	}
	if args.ForceROW == nil {
		args.ForceROW = &view.ForceROW
	}
	if args.EnableWriteCache == nil {
		args.EnableWriteCache = &view.EnableWriteCache
	}
	if args.FollowerRead == nil {
		args.FollowerRead = &view.FollowerRead
	}
	if args.RemoteCacheBoostEnable == nil {
		args.RemoteCacheBoostEnable = &view.RemoteCacheBoostEnable
	}
	if args.DpSelectorName == nil {
		args.DpSelectorName = &view.DpSelectorName
	}

	if cproto.IsRelease(cluster) {
		if *args.Capacity == view.Capacity && *args.EnableToken == view.EnableToken {
			return view, nil
		}
		if err = v.api.GetReleaseClient(cluster).UpdateVolView(args.Name, args.AuthKey, *args.Capacity, *args.EnableToken); err != nil {
			return nil, err
		}
		view.Capacity = *args.Capacity
		view.EnableToken = *args.EnableToken
		return view, nil
	} else {
		mc := v.api.GetMasterClient(cluster)
		vv, err := mc.AdminAPI().GetVolumeSimpleInfo(args.Name)
		if err != nil {
			log.LogErrorf("updateVolume: GetVolumeSimpleInfo failed, vol(%v)err(%v)", args.Name, err)
			return nil, err
		}
		if *args.Capacity == view.Capacity &&
			*args.DpReplicaNum == uint64(view.DpReplicaNum) &&
			*args.DefaultStoreMode == uint64(view.DefaultStoreMode) &&
			*args.TrashRemainingDays == uint64(view.TrashRemainingDays) &&
			*args.EnableToken == view.EnableToken &&
			*args.ForceROW == view.ForceROW &&
			*args.EnableWriteCache == view.EnableWriteCache &&
			*args.FollowerRead == view.FollowerRead &&
			*args.RemoteCacheBoostEnable == view.RemoteCacheBoostEnable &&
			*args.DpSelectorName == view.DpSelectorName {
			return view, nil
		}
		if err = mc.AdminAPI().UpdateVolume(vv.Name, *args.Capacity, int(*args.DpReplicaNum), int(vv.MpReplicaNum), int(*args.TrashRemainingDays),
			int(*args.DefaultStoreMode), *args.FollowerRead, vv.VolWriteMutexEnable, vv.NearRead, vv.Authenticate, *args.EnableToken, vv.AutoRepair,
			*args.ForceROW, vv.IsSmart, *args.EnableWriteCache, args.AuthKey, vv.ZoneName, "", strings.Join(vv.SmartRules, ","),
			uint8(vv.OSSBucketPolicy), uint8(vv.CrossRegionHAType), vv.ExtentCacheExpireSec, vv.CompactTag,
			vv.DpFolReadDelayConfig.DelaySummaryInterval, vv.FolReadHostWeight, vv.TrashCleanInterval, vv.BatchDelInodeCnt, vv.DelInodeInterval,
			vv.UmpCollectWay, vv.TrashCleanDuration, vv.TrashCleanMaxCount, vv.EnableBitMapAllocator,
			vv.RemoteCacheBoostPath, *args.RemoteCacheBoostEnable, vv.RemoteCacheAutoPrepare, vv.RemoteCacheTTL,
			vv.EnableRemoveDupReq, vv.ConnConfig.ConnectTimeoutNs, vv.ConnConfig.ReadTimeoutNs, vv.ConnConfig.WriteTimeoutNs, vv.TruncateEKCountEveryTime,
			vv.BitMapSnapFrozenHour, vv.NotCacheNode, vv.Flock, vv.MetaOut, vv.ReqRecordMaxCount, vv.ReqRecordReservedTime,
		); err != nil {
			log.LogErrorf("updateVolume failed")
			return view, err
		}
	}
	view, err = v.getVolumeView(cluster, args.Name)
	record := model.NewVolumeOperation(
		cluster,
		view.Name,
		proto.AdminUpdateVol,
		ctx.Value(cutil.PinKey).(string),
		view.String(),
	)
	record.InsertRecord(record)
	log.LogInfof("update vol: %v success", view.Name)
	return view, nil
}

func (v *VolumeService) filterVols(cluster, keywords string, page, pageSize int, listFunc getVolListFunc) (int, []*cproto.VolInfo, error) {
	vols, err := listFunc(cluster)
	if err != nil {
		return 0, nil, err
	}
	filterVols := make([]*cproto.VolInfo, 0)
	if keywords != "" {
		for _, vol := range vols {
			if strings.Contains(vol.Name, keywords) {
				filterVols = append(filterVols, vol)
			}
		}
	} else {
		filterVols = vols
	}
	total := len(filterVols)
	if total > 0 {
		filterVols = filterVols[(page-1)*pageSize : unit.Min(page*pageSize, total)]
	}
	return total, filterVols, nil
}

func (v *VolumeService) parallelGetVol(cluster string, vols []*cproto.VolInfo, volFunc getVolFunc) []*cproto.VolInfo {
	var (
		wg         sync.WaitGroup
		volChan    = make(chan *cproto.VolInfo, parallelThreadForGetVol)
		resultVol  = make([]*cproto.VolInfo, 0)
		resultLock sync.Mutex
	)

	wg.Add(1)
	go func(c chan<- *cproto.VolInfo) {
		defer wg.Done()
		for i, _ := range vols {
			c <- vols[i]
		}
		close(c)
	}(volChan)

	for i := 0; i < parallelThreadForGetVol; i++ {
		wg.Add(1)
		go func(c <-chan *cproto.VolInfo) {
			defer wg.Done()
			for {
				vol, ok := <-c
				if !ok {
					return
				}
				volInfo, isDeleted, err := volFunc(cluster, vol)
				if err == nil && !isDeleted {
					resultLock.Lock()
					resultVol = append(resultVol, volInfo)
					resultLock.Unlock()
				}
			}
		}(volChan)
	}
	wg.Wait()
	return resultVol
}

type getVolListFunc func(cluster string) ([]*cproto.VolInfo, error)
type getVolFunc func(cluster string, volInfo *cproto.VolInfo) (*cproto.VolInfo, bool, error)

func (v *VolumeService) GetVolList(cluster string) ([]*cproto.VolInfo, error) {
	result := make([]*cproto.VolInfo, 0)
	if cproto.IsRelease(cluster) {
		clusterView, err := v.api.GetClusterViewCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		for _, volStat := range clusterView.VolStat {
			newVolInfo := &cproto.VolInfo{
				Name:      volStat.Name,
				TotalSize: strconv.FormatUint(volStat.TotalGB, 10) + "GB",
				UsedSize:  strconv.FormatUint(volStat.UsedGB, 10) + "GB",
				UsedRatio: volStat.UsedRatio,
			}
			result = append(result, newVolInfo)
		}
	} else {
		volList, err := v.api.CacheManager.GetVolListCache(cluster, false)
		if err != nil {
			return nil, err
		}
		for _, volName := range volList {
			newVolInfo := &cproto.VolInfo{
				Name: volName,
			}
			result = append(result, newVolInfo)
		}
	}
	return result, nil
}

func (v *VolumeService) GetVol(cluster string, volBaseInfo *cproto.VolInfo) (*cproto.VolInfo, bool, error) {
	var markDeleted bool
	volInfo := new(cproto.VolInfo)
	if cproto.IsRelease(cluster) {
		rc := v.api.GetReleaseClient(cluster)
		volView, err := rc.AdminGetVol(volBaseInfo.Name)
		if err != nil {
			log.LogWarnf("VolumeService: GetVol failed, cluster(%v) vol(%v)", cluster, volInfo.Name)
			return nil, markDeleted, err
		}
		volInfo.Name = volView.Name
		volInfo.Owner = volView.Owner
		volInfo.CreateTime = ""
		volInfo.FileCount = volView.InodeCount
		volInfo.ReplicaNum = volView.DpReplicaNum
		volInfo.TotalSize = volBaseInfo.TotalSize
		volInfo.UsedSize = volBaseInfo.UsedSize
		volInfo.UsedRatio = volBaseInfo.UsedRatio
		if volView.Status == proto.VolStMarkDelete {
			markDeleted = true
		}
		volInfo.DpCount = int32(volView.DpCnt)
		volInfo.MpCount = int32(volView.MpCnt)
	} else {
		mc := v.api.GetMasterClient(cluster)
		volView, err := mc.AdminAPI().GetVolumeSimpleInfo(volBaseInfo.Name)
		if err != nil {
			log.LogWarnf("VolumeService: GetVol failed, cluster(%v) vol(%v)", cluster, volInfo.Name)
			return nil, false, err
		}
		volInfo.Name = volView.Name
		volInfo.Owner = volView.Owner
		volInfo.CreateTime = volView.CreateTime
		volInfo.FileCount = volView.InodeCount
		volInfo.ReplicaNum = volView.DpReplicaNum
		volInfo.TotalSize = cluster_service.FormatSize(volView.TotalSize)
		volInfo.UsedSize = cluster_service.FormatSize(volView.UsedSize)
		volInfo.UsedRatio = cluster_service.FormatRatio(volView.UsedRatio)
		if volView.Status == proto.VolStMarkDelete {
			markDeleted = true
		}
		volInfo.DpCount = int32(volView.DpCnt)
		volInfo.MpCount = int32(volView.MpCnt)
	}
	// 获取vol client数量(特别慢) -> 改成查表
	//clientCnt, err := v.vs.GetVolClientCount(cluster, volInfo.Name)
	table := model.ConsoleVolume{}
	volInfo.ClientCount = int32(table.GetVolumeClientCount(cluster, volInfo.Name))
	return volInfo, markDeleted, nil
}

func (v *VolumeService) getVolumeView(cluster, name string) (*cproto.Vol, error) {
	var vol *cproto.Vol
	if cproto.IsRelease(cluster) {
		rc := v.api.GetReleaseClient(cluster)
		volView, err := rc.AdminGetVol(name)
		if err != nil {
			log.LogErrorf("GetVolumeView failed: vol(%v)err(%v)", name, err)
			return nil, err
		}
		vol = &cproto.Vol{
			Name:         name,
			Owner:        volView.Owner,
			Capacity:     volView.Capacity,
			DpReplicaNum: volView.DpReplicaNum,
			EnableToken:  volView.EnableToken,
		}
	} else {
		mc := v.api.GetMasterClient(cluster)
		volView, err := mc.AdminAPI().GetVolumeSimpleInfo(name)
		if err != nil {
			log.LogErrorf("GetVolumeView failed: vol(%v)err(%v)", name, err)
			return nil, err
		}
		vol = &cproto.Vol{
			Name:                   name,
			Owner:                  volView.Owner,
			Capacity:               volView.Capacity,
			DpReplicaNum:           volView.DpReplicaNum,
			DefaultStoreMode:       uint8(volView.DefaultStoreMode),
			TrashRemainingDays:     uint8(volView.TrashRemainingDays),
			DpSelectorName:         volView.DpSelectorName,
			EnableToken:            volView.EnableToken,
			ForceROW:               volView.ForceROW,
			FollowerRead:           volView.FollowerRead,
			RemoteCacheBoostEnable: volView.RemoteCacheBoostEnable,
			EnableWriteCache:       volView.EnableWriteCache,
		}
	}
	return vol, nil
}

func (v *VolumeService) updateVolListCache(cluster, volName, flag string) {
	var err error
	if cproto.IsRelease(cluster) {
		err = v.api.CacheManager.UpdateClusterViewCache(cluster)
	} else {
		if flag == "create" {
			v.api.CacheManager.ManualAddVolToCache(cluster, volName)
		} else {
			_, err = v.api.CacheManager.GetVolListCache(cluster, true)
		}
	}
	if err != nil {
		log.LogErrorf("updateVolListCache: cluster(%s) volName(%s) flag(%s) err(%v)", cluster, volName, flag, err)
	}
	return
}

func (v *VolumeService) DownloadCfsClient(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return err
	}
	token, err := cutil.Token(r)
	if err != nil {
		return err
	}
	if _, err = cutil.TokenValidate(token); err != nil {
		return err
	}
	volName := r.Form.Get("vol")
	if volName == "" {
		return fmt.Errorf("not found param[vol]")
	}
	cluster := r.Form.Get("cluster")
	if cluster == "" {
		return fmt.Errorf("not found param[cluster]")
	}
	absPath, err := v.vs.GetVolClientListCSV(cluster, volName)
	if err != nil {
		log.LogErrorf("DownloadCfsClient failed: cluster(%v) volName(%v) err(%v)", cluster, volName, err)
		return err
	}
	file, err := os.Open(absPath)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	_, filename := path.Split(absPath)
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))

	file.Seek(0, 0)
	_, err = io.Copy(w, file)
	if err != nil {
		log.LogErrorf("DownloadCfsClient: copy file to http.writer failed: %v", err)
	}
	return nil
}

func formatPartitionStatus(status int8) string {
	switch status {
	case 1:
		return "ReadOnly"
	case 2:
		return "Writable"
	case -1:
		return "Unavailable"
	default:
		return "Unknown"
	}
}
