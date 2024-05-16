package file

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"os"
	"path"
	"strings"
)

type taskType uint8

const (
	ListTrash taskType = iota
	RecoverTrash
	CleanTrash
	DeleteFile
	ListDir
)

type TrashTask struct {
	TaskType taskType
	Volume   string
	Cluster  string
	Path     string // abs dentry name
	Inode    uint64
	Prev     string // token name
	Ts       int64
	Batch    int64 // list file batchNum
	err      chan error
	result   chan interface{}
}

func (t *TrashTask) String() string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf("{type(%v) volume(%v) cluster(%v) path(%v) inode(%v) prev(%v) ts(%v) batch(%v)}",
		t.TaskType, t.Volume, t.Cluster, t.Path, t.Inode, t.Prev, t.Ts, t.Batch)
}

func (m *TrashManager) ListTrashTaskHandler(task *TrashTask) (ddentrys []*proto.DeletedDentry, err error) {
	var (
		v     *volume
		inode uint64
	)
	v, err = m.loadVolume(task.Volume, task.Cluster)
	if err != nil {
		err = fmt.Errorf("load volume failed: task(%v)", task)
		return
	}
	if task.Inode != 0 {
		inode = task.Inode
	} else {
		inode, _, err = v.getInodeFromPath(task.Path)
		if err != nil {
			err = fmt.Errorf("getInodeFromPath failed: path(%v), err(%v)", task.Path, err)
			return
		}
	}
	// 一直拉取 直到没有nextToken
	mp := v.getMetaPartitionByInode(inode)
	var prev *proto.DeletedDentry
	ddentrys = make([]*proto.DeletedDentry, 0)
	for {
		dens, err := getAllDeletedDentrysByParentIno(mp.GetLeaderAddr(), string(m.metaProf[task.Cluster]), mp.PartitionID, inode, prev)
		if err != nil {
			break
		}
		if len(dens) < proto.ReadDeletedDirBatchNum {
			ddentrys = append(ddentrys, dens...)
			break
		}
		prev = dens[len(dens)-1]
		ddentrys = append(ddentrys, dens[:len(dens)-1]...)
	}
	if err != nil {
		log.LogErrorf("ListTrashTaskHandler failed: err(%v)", err)
		return nil, err
	}
	return
}

func (m *TrashManager) ListDirTaskHandler(task *TrashTask) (dentrys []proto.Dentry, err error) {
	var (
		v    *volume
		pIno uint64
	)
	v, err = m.loadVolume(task.Volume, task.Cluster)
	if err != nil {
		err = fmt.Errorf("load volume failed: task(%v)", task)
		return
	}
	if task.Inode != 0 {
		pIno = task.Inode
	} else {
		var isDel bool
		pIno, isDel, err = v.getInodeFromPath(task.Path)
		if err != nil {
			err = fmt.Errorf("getInodeFromPath failed: path(%v), err(%v)", task.Path, err)
			return
		}
		if isDel {
			err = fmt.Errorf("getInodeFromPath: path[%v] is deleted", task.Path)
			log.LogWarnf("ListDirTaskHandler: %v", err)
			return
		}
	}
	var next string
	dens := make([]proto.Dentry, 0)
	for {
		dens, next, err = v.mw.ReadDir_wo(pIno, "", next, 0)
		if err != nil || len(dens) == 0 {
			break
		}
		for _, den := range dens {
			if !os.FileMode(den.Type).IsDir() {
				continue
			} else {
				den.Type = 1
			}
			dentrys = append(dentrys, den)
		}
		if next == "" {
			break
		}
	}
	if err != nil {
		log.LogErrorf("ListDirTaskHandler failed: err(%v)", err)
		return nil, err
	}
	return
}

func (m *TrashManager) RecoverTaskHandler(task *TrashTask) (isSuccess bool, err error) {
	var v *volume
	v, err = m.loadVolume(task.Volume, task.Cluster)
	if err != nil {
		err = fmt.Errorf("load volume failed task(%v)", task)
		return
	}
	absPath := path.Clean(task.Path)
	if strings.HasPrefix(absPath, "/") == false {
		err = fmt.Errorf("the path[%v] is invalid", absPath)
		return
	}
	dir, name := path.Split(absPath)
	var (
		parentID uint64
	)
	parentID, err = v.recoverParentDir(dir)
	if err != nil {
		err = fmt.Errorf("recover parentDir failed: path[%s] file[%s]", dir, name)
		return
	}
	err = v.doRecoverPath(parentID, name)
	if err != nil {
		err = fmt.Errorf("doRecover path failed: pIno[%v] name[%s]", parentID, name)
		return
	}
	return true, nil
}

// 整个卷的回收站清空
func (m *TrashManager) CleanTrashTaskHandler(task *TrashTask) {
	var (
		v   *volume
		err error
	)
	v, err = m.loadVolume(task.Volume, task.Cluster)
	if err != nil {
		log.LogErrorf("load volume failed task(%v)", task)
		return
	}
	ino := rootIno
	v.doCleanPath(task.Path, ino, m.metaProf[task.Cluster])
	return
}

func (m *TrashManager) loadVolume(volName string, cluster string) (vol *volume, err error) {
	var (
		client *master.MasterClient
	)
	m.RLock()
	defer m.RUnlock()
	client = m.masterClient[cluster]
	if client == nil {
		err = fmt.Errorf("loadVolume failed: master client is nil")
		return nil, err
	}
	loader, ok := m.volManager[cluster]
	if !ok {
		err = fmt.Errorf("can't find cluster[%v] info", cluster)
		log.LogErrorf("loadRecoverEnv: err(%v)", err)
		return nil, err
	}
	value, ok := loader.volumes.Load(volName)
	if !ok {
		vol = NewVolume(volName, client)
		if vol == nil {
			err = fmt.Errorf("NewVolume failed, cluster(%v) vol(%v)", cluster, volName)
			log.LogErrorf("loadRecoverEnv: err(%v)", err)
			return nil, err
		}
		loader.volumes.Store(volName, vol)
		return vol, nil
	}
	vol = value.(*volume)
	return vol, nil
}
