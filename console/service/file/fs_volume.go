package file

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"math"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	rootIno                    = proto.RootIno
	MAXVolumeTTL               = time.Duration(time.Minute * 60)
	CleanExpiredVolumeDuration = time.Duration(time.Minute * 10)
)

var (
	ctx                    = context.Background()
	RegexpFileIsDeleted, _ = regexp.Compile("_(\\d{14})+.+(\\d{6})+$")
	dentryNameTimeFormat   = "20060102150405.000000"
)

type volume struct {
	name       string
	mw         *meta.MetaWrapper
	ec         *data.ExtentClient
	createTime time.Time //  创建时间 TTL 一个卷最多留在里面1小时

	closeOnce sync.Once
	closeCh   chan struct{}
}

func NewVolume(volName string, mc *master.MasterClient) *volume {
	metaConfig := new(meta.MetaConfig)
	metaConfig.Masters = mc.Nodes()
	metaConfig.Volume = volName
	var (
		metaWrapper *meta.MetaWrapper
		err         error
	)
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		log.LogErrorf("NewVolume: new metaWrapper failed, err: %v", err)
		return nil
	}
	vol := &volume{
		name: volName,
		mw:   metaWrapper,

		closeCh:    make(chan struct{}),
		createTime: time.Now(),
	}
	return vol
}

func (v *volume) Close() error {
	v.closeOnce.Do(func() {
		close(v.closeCh)
		_ = v.mw.Close()
	})
	return nil
}

func (v *volume) recoverParentDir(p string) (ino uint64, err error) {
	var (
		child              uint64
		dentrys            []*proto.DeletedDentry
		startTime, endTime int64
	)
	ino = rootIno
	dirs := strings.Split(p, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, _, err = v.mw.Lookup_ll(ctx, ino, dir)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("recoverParentDir-Lookup_ll failed, pIno(%v) name(%v) err(%v)", ino, dir, err)
			return
		} else if err == syscall.ENOENT {
			startTime = 0
			endTime = math.MaxInt64
			// ino ParentIno
			if RegexpFileIsDeleted.MatchString(dir) {
				dir, startTime, err = parseDeletedName(dir)
				if err != nil {
					return
				}
				endTime = startTime
			}
			dentrys, err = v.mw.LookupDeleted_ll(ctx, ino, dir, startTime, endTime)
			if err != nil {
				log.LogErrorf("recoverParentDir-LookupDeleted_ll failed: ino[%v] name[%v] err[%s]", ino, dir, err.Error())
				return
			}
			if len(dentrys) > 1 {
				err = fmt.Errorf("recoverParentDir-the dir[%s] has multiple deleted records with timestamps", dir)
				log.LogErrorf("recoverParentDir failed: err[%s]", err)
				return
			}
			child = dentrys[0].Inode
			err = recoverFile(ino, dentrys[0], v.mw)
			if err != nil {
				log.LogErrorf(err.Error())
				return
			}
		}
		ino = child
	}
	return
}

func (v *volume) doRecoverPath(parentID uint64, name string) (err error) {
	var (
		startTime, endTime int64
		dentrys            []*proto.DeletedDentry
	)
	startTime = 0
	endTime = math.MaxInt64
	if RegexpFileIsDeleted.MatchString(name) {
		name, startTime, err = parseDeletedName(name)
		if err != nil {
			return
		}
		endTime = startTime
	}
	dentrys, err = v.mw.LookupDeleted_ll(ctx, parentID, name, startTime, endTime)
	log.LogInfof("doRecoverPath-lookupDeleted: dentrys[%v] err[%v] parentID[%v] name[%v]", dentrys, err, parentID, name)
	if err == nil {
		// 普通文件
		if len(dentrys) > 1 {
			// 说明该文件有多删除的版本
			err = fmt.Errorf("doRecoverPath-the file[%s] has multiple deleted records with timestamps", name)
			log.LogErrorf(err.Error())
			return
		}
		// 不会出现err == nil && len(dentrys)==0这种情况吧？
		err = recoverFile(parentID, dentrys[0], v.mw)
		if err != nil {
			log.LogErrorf("doRecoverPath-recover failed: pIno[%v] dentrys[%v]", parentID, dentrys[0])
			return
		}
		if proto.IsDir(dentrys[0].Type) {
			err = recoverDirectory(dentrys[0], v.mw)
			if err != nil {
				log.LogErrorf("doRecoverPath-recoverDirectory failed: dentrys[%v]", dentrys[0])
				return
			}
		}
		return
	} else if err == syscall.ENOENT {
		// 目录
		ino, fType, err1 := v.mw.Lookup_ll(ctx, parentID, name)
		if err1 != nil {
			err = fmt.Errorf("doRecoverPath-Lookup_ll failed: pIno[%v] name[%s] err[%s]", parentID, name, err1.Error())
			return
		}
		if proto.IsDir(fType) == false {
			return
		}
		var dd *proto.DeletedDentry
		dd = &proto.DeletedDentry{
			Inode: ino,
		}
		err = recoverDirectory(dd, v.mw)
		if err != nil {
			return
		}
		return
	} else {
		// 失败
		log.LogErrorf("doRecoverPath-LookupDeleted_ll failed: err[%s]", err.Error())
		return
	}
}

func (v *volume) doCleanPath(path string, ino uint64, port string) {
	mp := v.getMetaPartitionByInode(ino)
	leader := mp.GetLeaderAddr()

	inodes := make([]uint64, 0, proto.ReadDeletedDirBatchNum)

	var prev *proto.DeletedDentry
	for {
		dens, err := getAllDeletedDentrysByParentIno(leader, port, mp.PartitionID, ino, prev)
		if err != nil {
			log.LogErrorf("doCleanPath: getDeletedDentrysByParentIno failed: %v", err)
			break
		}
		if len(dens) == proto.ReadDeletedDirBatchNum {
			prev = dens[proto.ReadDeletedDirBatchNum-1]
			dens = dens[:proto.ReadDeletedDirBatchNum-1]
		}
		for _, den := range dens {
			inodes = append(inodes, den.Inode)
			if proto.IsDir(den.Type) == false {
				continue
			}
			v.recursiveCleanDir(den)
		}
		// batchDeleted
		_, err = v.mw.BatchCleanDeletedInode(ctx, inodes)
		_, err = v.mw.BatchCleanDeletedDentry(ctx, dens)
		if prev == nil {
			break
		}
		inodes = inodes[:0]
	}
	// lookupDir
	dirs, err := v.lookupPathForDir(ctx, ino)
	if err != nil {
		log.LogErrorf("doCleanPath: lookupPathForDir failed: %v", err)
		return
	}
	for _, dir := range dirs {
		v.doCleanPath(dir.Name, dir.Inode, port)
	}
}

func (v *volume) getInodeFromPath(p string) (ino uint64, isDeleted bool, err error) {
	var (
		child              uint64
		dentrys            []*proto.DeletedDentry
		startTime, endTime int64
	)
	if p == "/" {
		return rootIno, isDeleted, nil
	}
	ino = rootIno
	p = path.Clean(p)
	dirs := strings.Split(p, "/")
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if isDeleted {
			startTime = 0
			endTime = math.MaxInt64
			// name 正则
			if RegexpFileIsDeleted.MatchString(dir) {
				dir, startTime, err = parseDeletedName(dir)
				if err != nil {
					return
				}
				endTime = startTime
			}
			dentrys, err = v.mw.LookupDeleted_ll(ctx, ino, dir, startTime, endTime)
			if err != nil {
				log.LogErrorf("getInodeFromPath-LookupDeleted_ll failed: pIno(%v) name(%v) err(%v)", ino, dir, err)
				return 0, isDeleted, err
			}
			if len(dentrys) > 1 {
				err = fmt.Errorf("getInodeFromPath-the dir[%s] has multiple deleted records with timestamps", dir)
				log.LogErrorf("getInodeFromPath failed: err[%s]", err)
				return 0, isDeleted, err
			}
			isDeleted = true
			child = dentrys[0].Inode
		} else {
			child, _, err = v.mw.Lookup_ll(ctx, ino, dir)
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("getInodeFromPath-Lookup_ll failed: pIno(%v) name(%v) err(%v)", ino, dir, err)
				return 0, isDeleted, err
			} else if err == syscall.ENOENT {
				startTime = 0
				endTime = math.MaxInt64
				if RegexpFileIsDeleted.MatchString(dir) {
					dir, startTime, err = parseDeletedName(dir)
					if err != nil {
						return
					}
					endTime = startTime
				}
				dentrys, err = v.mw.LookupDeleted_ll(ctx, ino, dir, startTime, endTime)
				if err != nil {
					log.LogErrorf("getInodeFromPath-LookupDeleted_ll failed: pIno(%v) name(%v) err(%v)", ino, dir, err)
					return 0, isDeleted, err
				}
				if len(dentrys) > 1 {
					err = fmt.Errorf("getInodeFromPath-the dir[%s] has multiple deleted records with timestamps", dir)
					log.LogErrorf("getInodeFromPath failed: err[%s]", err)
					return 0, isDeleted, err
				}
				isDeleted = true
				child = dentrys[0].Inode
			}
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("getInodeFromPath: pIno(%v) name(%v) inode(%v) isDeleted(%v)", ino, dir, child, isDeleted)
		}
		ino = child
	}
	log.LogInfof("getInodeFromPath: path[%v]-inode[%v], isDeleted(%v)", p, ino, isDeleted)
	return
}

func (v *volume) getMetaPartitionByInode(inode uint64) (mp *meta.MetaPartition) {
	// expose func
	return v.mw.GetPartitionByInode(inode)
}

func recoverDirectory(dd *proto.DeletedDentry, mw *meta.MetaWrapper) (err error) {
	pIno := dd.Inode
	var children []*proto.DeletedDentry
	children, err = mw.ReadDirDeleted(ctx, pIno)
	if err != nil {
		log.LogErrorf("recoverDirectory-ReadDirDeleted failed: ino[%v] err[%s]", pIno, err.Error())
		return
	}

	if len(children) == 0 {
		// 这个目录本身在 recoverParentDir的时候就恢复了
		return
	}
	inodes := make([]uint64, 0, len(children))
	for _, child := range children {
		inodes = append(inodes, child.Inode)
	}

	res := make(map[uint64]int, 0)
	res, err = mw.BatchRecoverDeletedInode(ctx, inodes)
	if err != nil {
		log.LogErrorf("recoverDirectory-BatchRecoverDeletedInode failed: dentry[%v] err[%v]", dd, err.Error())
		return
	}

	dens := make([]*proto.DeletedDentry, 0, len(children))
	for _, child := range children {
		st, ok := res[child.Inode]
		if ok && st != meta.StatusOK && st != meta.StatusExist {
			continue
		}
		dens = append(dens, child)
	}
	if len(dens) == 0 {
		return
	}

	res = make(map[uint64]int, 0)
	res, err = mw.BatchRecoverDeletedDentry(ctx, dens)
	if err != nil {
		log.LogErrorf("recoverDirectory-BatchRecoverDeletedDentry failed: dentry[%v] err[%v]", dd, err.Error())
		return
	}
	//递归恢复
	for _, chd := range dens {
		if !proto.IsDir(chd.Type) {
			continue
		}
		st, ok := res[chd.Inode]
		if ok && st != meta.StatusOK && st != meta.StatusExist {
			continue
		}
		err = recoverDirectory(chd, mw)
		if err != nil {
			log.LogErrorf("doRecoverOneDirectory, failed to doRecoverOneDirectory, dentry: %v, err: %v", chd, err.Error())
		}
	}
	return
}

func recoverFile(pid uint64, d *proto.DeletedDentry, mw *meta.MetaWrapper) (err error) {
	err = mw.RecoverDeletedInode(ctx, d.Inode)
	if err == syscall.ENOENT {
		log.LogWarnf("recover deleted inode for dentry: %v, pid: %v, err: %v", d, pid, err.Error())
		return nil
	} else if err != nil {
		err = fmt.Errorf("failed to recover deleted INode for dentry: %v, pid: %v, err: %v", d, pid, err.Error())
		return
	}
	err = mw.RecoverDeletedDentry(ctx, pid, d.Inode, d.Name, d.Timestamp)
	if err != nil && err != syscall.EEXIST {
		err = fmt.Errorf("failed to recover deleted dentry: %v, pid: %v, err: %v", d, pid, err.Error())
		return
	}
	return nil
}

// 删除的目录
func (v *volume) recursiveCleanDir(dentry *proto.DeletedDentry) {
	children, err := v.mw.ReadDirDeleted(ctx, dentry.Inode)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	inodes := make([]uint64, 0, len(children))
	for _, chd := range children {
		if proto.IsDir(chd.Type) == true {
			v.recursiveCleanDir(chd)
		}
		inodes = append(inodes, chd.Inode)
	}
	_, err = v.mw.BatchCleanDeletedInode(ctx, inodes)
	_, err = v.mw.BatchCleanDeletedDentry(ctx, children)
}

func (v *volume) lookupPathForDir(ctx context.Context, ino uint64) (dir []*proto.Dentry, err error) {
	dentrys, err := v.mw.ReadDir_ll(ctx, ino)
	if err != nil {
		return
	}
	if len(dentrys) == 0 {
		return
	}
	total := 0
	dir = make([]*proto.Dentry, 0, len(dentrys))
	for _, den := range dentrys {
		if !os.FileMode(den.Type).IsDir() {
			continue
		}
		dir = append(dir, &den)
		total++
	}
	return
}

func parseDeletedName(str string) (name string, ts int64, err error) {
	index := strings.LastIndex(str, "_")
	name = str[0:index]
	date := str[index+1:]
	loc, _ := time.LoadLocation("Local")
	var tm time.Time
	tm, err = time.ParseInLocation(dentryNameTimeFormat, date, loc)
	if err != nil {
		log.LogErrorf("parseDeletedName: str: %v, err: %v", str, err.Error())
		return
	}
	ts = tm.UnixNano() / 1000
	return
}

type VolumeLoader struct {
	cluster string
	masters []string
	volumes sync.Map     // volName -> volume
	ticker  *time.Ticker // 定时清理 避免太多的volume信息  30分钟没用的就移除

	closeOnce sync.Once // 用于清理
	closeCh   chan struct{}
}

func NewVolumeLoader(cluster string, masters []string) *VolumeLoader {
	vl := &VolumeLoader{
		cluster: cluster,
		masters: masters,
		closeCh: make(chan struct{}),
	}

	go vl.updateVolume()

	return vl
}

func (vl *VolumeLoader) updateVolume() {
	vl.ticker = time.NewTicker(CleanExpiredVolumeDuration)
	defer vl.ticker.Stop()
	for {
		select {
		case <-vl.ticker.C:
			vl.volumes.Range(func(key, value interface{}) bool {
				name, _ := key.(string)
				vol, ok := value.(volume)
				if !ok || time.Since(vol.createTime) > MAXVolumeTTL {
					vl.cleanExpireVolume(name)
				}
				return true
			})
		case <-vl.closeCh:
			return
		}
	}
}

func (vl *VolumeLoader) cleanExpireVolume(volName string) {
	value, ok := vl.volumes.Load(volName)
	if ok {
		vl.volumes.Delete(volName)
		log.LogDebugf("VolumeLoader: cleanExpireVolume, vol(%v)", volName)
	}
	vol, ok := value.(*volume)
	if closeErr := vol.Close(); closeErr != nil {
		log.LogErrorf("VolumeLoader: cleanExpireVolume, vol%v), err(%v)", vol, closeErr)
	}
}
