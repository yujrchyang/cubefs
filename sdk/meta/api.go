// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package meta

import (
	"context"
	"fmt"
	syslog "log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/util/bloomfilter"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/multipart"
)

// Low-level API, i.e. work with inode

const (
	BatchIgetRespBuf = 1000
	BatchIgetLimit   = 2000

	CreateSubDirDeepLimit = 5
)

const (
	OpenRetryInterval = 5 * time.Millisecond
	OpenRetryLimit    = 1000
)

func (mw *MetaWrapper) GetRootIno(subdir string, autoMakeDir bool) (uint64, error) {
	rootIno := proto.RootIno
	if subdir == "" || subdir == "/" {
		return rootIno, nil
	}

	dirs := strings.Split(subdir, "/")
	dirDeep := len(dirs)
	for idx, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, mode, err := mw.Lookup_ll(context.Background(), rootIno, dir)
		if err != nil {
			if autoMakeDir && err == syscall.ENOENT && (dirDeep-idx) < CreateSubDirDeepLimit {
				// create directory
				if rootIno, err = mw.makeDirectory(rootIno, dir); err == nil {
					continue
				}
			}
			return 0, fmt.Errorf("GetRootIno: Lookup failed, subdir(%v) idx(%v) dir(%v) err(%v)", subdir, idx, dir, err)
		}
		if !proto.IsDir(mode) {
			return 0, fmt.Errorf("GetRootIno: not directory, subdir(%v) idx(%v) dir(%v) child(%v) mode(%v) err(%v)", subdir, idx, dir, child, mode, err)
		}
		rootIno = child
	}
	syslog.Printf("GetRootIno: inode(%v) subdir(%v)\n", rootIno, subdir)
	return rootIno, nil
}

// Looks up absolute path and returns the ino
func (mw *MetaWrapper) LookupPath(ctx context.Context, rootIno uint64, subdir string) (uint64, error) {
	ino := rootIno
	if subdir == "" || subdir == "/" {
		return ino, nil
	}

	dirs := strings.Split(subdir, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, _, err := mw.Lookup_ll(ctx, ino, dir)
		if err != nil {
			return 0, err
		}
		ino = child
	}
	return ino, nil
}

func (mw *MetaWrapper) makeDirectory(parIno uint64, dirName string) (uint64, error) {
	inodeInfo, err := mw.Create_ll(context.Background(), parIno, dirName, proto.Mode(os.ModeDir|0755), 0, 0, nil)
	if err != nil {
		if err == syscall.EEXIST {
			existInode, existMode, e := mw.Lookup_ll(context.Background(), parIno, dirName)
			if e == nil && proto.IsDir(existMode) {
				return existInode, nil
			}
			return 0, fmt.Errorf("MakeDirectory failed: create err(%v) lookup err(%v) existInode(%v) existMode(%v)", err, e, existInode, existMode)
		}
		return 0, fmt.Errorf("MakeDirectory failed: create err(%v)", err)
	}
	if !proto.IsDir(inodeInfo.Mode) {
		return 0, fmt.Errorf("MakeDirectory failed: inode mode invalid")
	}
	return inodeInfo.Inode, nil
}

func (mw *MetaWrapper) Statfs() (total, used uint64) {
	total = atomic.LoadUint64(&mw.totalSize)
	used = atomic.LoadUint64(&mw.usedSize)
	return
}

func (mw *MetaWrapper) Create_ll(ctx context.Context, parentID uint64, name string, mode, uid, gid uint32, target []byte) (info *proto.InodeInfo, err error) {
	if mw.VolNotExists() {
		return nil, proto.ErrVolNotExists
	}
	var (
		status int
		errmsg string
		mp     *MetaPartition
	)
	defer func() {
		if err != nil && err != syscall.ENOENT && err != syscall.EEXIST {
			log.LogErrorf("Create_ll: vol(%v) parentID(%v) name(%v) mode(%v) uid(%v) gid(%v) target(%v) err(%v) errmsg(%v) status(%v)", mw.volname, parentID, name, mode, uid, gid, target, err, errmsg, status)
		}
		if err == nil && mw.RemoteCacheBloom != nil && info != nil {
			cacheBloom := mw.RemoteCacheBloom()
			if bloomfilter.CheckUint64Exist(cacheBloom, parentID) {
				bloomfilter.AddUint64ToBloom(cacheBloom, info.Inode)
			}
		}
	}()

	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	// Create Inode
	var icreateFunc operatePartitionFunc = func(mp1 *MetaPartition) (bool, int) {
		status, info, err = mw.icreate(ctx, mp1, mode, uid, gid, target)
		if err == nil && status == statusOK {
			mp = mp1
			return true, status
		} else {
			return false, status
		}
	}
	retryCount := 0
	for {
		retryCount++
		if mw.iteratePartitions(icreateFunc) {
			goto create_dentry
		}
		if !mw.InfiniteRetry {
			errmsg = "create inode failed"
			return nil, syscall.ENOMEM
		}
		log.LogWarnf("Create_ll: create inode failed, vol(%v) err(%v) status(%v) parentID(%v) name(%v) retry time(%v)", mw.volname, err, status, parentID, name, retryCount)
		umpMsg := fmt.Sprintf("CreateInode err(%v) status(%v) parentID(%v) name(%v) retry time(%v)", err, status, parentID, name, retryCount)
		common.HandleUmpAlarm(mw.cluster, mw.volname, "CreateInode", umpMsg)
		time.Sleep(SendRetryInterval)
	}

create_dentry:
	status, err = mw.dcreate(ctx, parentMP, parentID, name, info.Inode, mode)
	if err == nil && status == statusOK {
		return info, nil
	}

	if err == nil && status == statusExist {
		newStatus, oldInode, mode, newErr := mw.lookup(ctx, parentMP, parentID, name)
		if newErr == nil && newStatus == statusOK {
			if oldInode == info.Inode {
				return info, nil
			}
			if mw.inodeNotExist(ctx, oldInode) {
				updateStatus, _, updateErr := mw.dupdate(ctx, parentMP, parentID, name, info.Inode)
				if updateErr == nil && updateStatus == statusOK {
					log.LogWarnf("Create_ll: inode(%v) is not exist, update dentry to new inode(%v) parentID(%v) name(%v)",
						oldInode, info.Inode, parentID, name)
					return info, nil
				}
				log.LogWarnf("Create_ll: update_dentry failed, status(%v), err(%v)", updateStatus, updateErr)
			} else {
				mw.iunlink(ctx, mp, info.Inode, true)
				mw.ievict(ctx, mp, info.Inode, true)
				log.LogWarnf("Create_ll: dentry has allready been created by other client, curInode(%v), mode(%v)",
					oldInode, mode)
			}
		} else {
			log.LogWarnf("Create_ll: check create_dentry failed, status(%v), err(%v)", newStatus, newErr)
		}
	}

	// Unable to determin create dentry status, rollback may cause unexcepted result.
	// So we return status error, user should check opration result manually or retry.
	return nil, statusToErrno(status)
}

func (mw *MetaWrapper) inodeNotExist(ctx context.Context, inode uint64) bool {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return false
	}
	status, _, _, err := mw.iget(ctx, mp, inode, false)
	return err == nil && status == statusNoent
}

func (mw *MetaWrapper) Lookup_ll(ctx context.Context, parentID uint64, name string) (inode uint64, mode uint32, err error) {
	if mw.VolNotExists() {
		return 0, 0, proto.ErrVolNotExists
	}
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("Lookup_ll: parentID(%v) name(%v) err(%v) status(%v)", parentID, name, err, status)
		}
		if err == nil && mw.RemoteCacheBloom != nil {
			cacheBloom := mw.RemoteCacheBloom()
			if bloomfilter.CheckUint64Exist(cacheBloom, parentID) {
				bloomfilter.AddUint64ToBloom(cacheBloom, inode)
			}
		}
	}()

	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return 0, 0, syscall.ENOENT
	}

	status, inode, mode, err = mw.lookup(ctx, parentMP, parentID, name)
	if err != nil || status != statusOK {
		return 0, 0, statusToErrno(status)
	}
	return inode, mode, nil
}

func (mw *MetaWrapper) InodeGet_ll(ctx context.Context, inode uint64) (info *proto.InodeInfo, err error) {
	if mw.VolNotExists() {
		return nil, proto.ErrVolNotExists
	}
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("InodeGet_ll: vol(%v) ino(%v) err(%v) status(%v)", mw.volname, inode, err, status)
		}
	}()

	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return nil, syscall.ENOENT
	}

	status, info, _, err = mw.iget(ctx, mp, inode, false)
	if err != nil || status != statusOK {
		if status == statusNoent {
			// For NOENT error, pull the latest mp and give it another try,
			// in case the mp view is outdated.
			mw.triggerAndWaitForceUpdate()
			info, _, err = mw.doInodeGet(ctx, inode, false)
			return info, err
		}
		return nil, statusToErrno(status)
	}
	if proto.IsSymlink(info.Mode) && info.Target != nil {
		info.Size = uint64(len(*info.Target))
	}
	log.LogDebugf("InodeGet_ll: info(%v)", info)
	return info, nil
}

func (mw *MetaWrapper) InodeGetWithXattrs(ctx context.Context, inode uint64) (info *proto.InodeInfo, xattrs []*proto.ExtendAttrInfo, err error) {
	if mw.VolNotExists() {
		return nil, nil, proto.ErrVolNotExists
	}
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("InodeGetWithXattrs: vol(%v) ino(%v) err(%v) status(%v)", mw.volname, inode, err, status)
		}
	}()

	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return nil, nil, syscall.ENOENT
	}

	status, info, xattrs, err = mw.iget(ctx, mp, inode, true)
	if err != nil || status != statusOK {
		if status == statusNoent {
			// For NOENT error, pull the latest mp and give it another try,
			// in case the mp view is outdated.
			mw.triggerAndWaitForceUpdate()
			info, xattrs, err = mw.doInodeGet(ctx, inode, true)
			return info, xattrs, err
		}
		return nil, nil, statusToErrno(status)
	}
	if proto.IsSymlink(info.Mode) {
		info.Size = uint64(len(*info.Target))
	}
	log.LogDebugf("InodeGetWithXattrs: info(%v)", info)
	return info, xattrs, nil
}

// Just like InodeGet but without retry
func (mw *MetaWrapper) doInodeGet(ctx context.Context, inode uint64, withXattrs bool) (*proto.InodeInfo, []*proto.ExtendAttrInfo, error) {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return nil, nil, syscall.ENOENT
	}

	status, info, xattrs, err := mw.iget(ctx, mp, inode, withXattrs)
	if err != nil || status != statusOK {
		return nil, nil, statusToErrno(status)
	}
	log.LogDebugf("doInodeGet: info(%v)", info)
	return info, xattrs, nil
}

func (mw *MetaWrapper) BatchInodeGet(ctx context.Context, inodes []uint64) []*proto.InodeInfo {
	var wg sync.WaitGroup

	batchInfos := make([]*proto.InodeInfo, 0)
	resp := make(chan []*proto.InodeInfo, BatchIgetRespBuf)
	candidates := make(map[uint64][]uint64)

	// Target partition does not have to be very accurate.
	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	var needForceUpdate = false
	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			needForceUpdate = true
			continue
		}
		wg.Add(1)
		go mw.batchIget(ctx, &wg, mp, inos, resp)
	}
	if needForceUpdate {
		mw.triggerForceUpdate()
	}

	go func() {
		wg.Wait()
		close(resp)
	}()

	for infos := range resp {
		batchInfos = append(batchInfos, infos...)
	}
	return batchInfos
}

// InodeDelete_ll is a low-level api that removes specified inode immediately
// and do not effect extent data managed by this inode.
func (mw *MetaWrapper) InodeDelete_ll(ctx context.Context, inode uint64) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("InodeDelete_ll: vol(%v) ino(%v) err(%v) status(%v)", mw.volname, inode, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}
	status, err = mw.idelete(ctx, mp, inode)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
	}
	log.LogDebugf("InodeDelete_ll: inode(%v)", inode)
	return
}

func (mw *MetaWrapper) BatchGetXAttr(ctx context.Context, inodes []uint64, keys []string) ([]*proto.XAttrInfo, error) {
	// Collect meta partitions
	var (
		mps      = make(map[uint64]*MetaPartition) // Mapping: partition ID -> partition
		mpInodes = make(map[uint64][]uint64)       // Mapping: partition ID -> inodes
	)
	for _, ino := range inodes {
		var mp = mw.getPartitionByInode(ctx, ino)
		if mp != nil {
			mps[mp.PartitionID] = mp
			mpInodes[mp.PartitionID] = append(mpInodes[mp.PartitionID], ino)
		}
	}

	var (
		xattrsCh = make(chan *proto.XAttrInfo, len(inodes))
		errorsCh = make(chan error, len(inodes))
	)

	var wg sync.WaitGroup
	for pID := range mps {
		wg.Add(1)
		go func(mp *MetaPartition, inodes []uint64, keys []string) {
			defer wg.Done()
			xattrs, err := mw.batchGetXAttr(ctx, mp, inodes, keys)
			if err != nil {
				errorsCh <- err
				log.LogErrorf("BatchGetXAttr: get xattr fail: volume(%v) partitionID(%v) inodes(%v) keys(%v) err(%s)",
					mw.volname, mp.PartitionID, inodes, keys, err)
				return
			}
			for _, info := range xattrs {
				xattrsCh <- info
			}
		}(mps[pID], mpInodes[pID], keys)
	}
	wg.Wait()

	close(xattrsCh)
	close(errorsCh)

	if len(errorsCh) > 0 {
		return nil, <-errorsCh
	}

	var xattrs = make([]*proto.XAttrInfo, 0, len(inodes))
	for {
		info := <-xattrsCh
		if info == nil {
			break
		}
		xattrs = append(xattrs, info)
	}
	return xattrs, nil
}

/*
 * Note that the return value of InodeInfo might be nil without error,
 * and the caller should make sure InodeInfo is valid before using it.
 */
func (mw *MetaWrapper) Delete_ll(ctx context.Context, parentID uint64, name string, isDir bool) (info *proto.InodeInfo, err error) {
	var (
		errmsg string
		status int
		inode  uint64
		mode   uint32
		mp     *MetaPartition
	)
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("Delete_ll: vol(%v) parentID(%v) name(%v) ino(%v) isDir(%v) mode(%v) err(%v) errmsg(%v) status(%v)", mw.volname, parentID, name, inode, isDir, mode, err, errmsg, status)
		}
	}()
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	if isDir {
		status, inode, mode, err = mw.lookup(ctx, parentMP, parentID, name)
		if err != nil || status != statusOK {
			errmsg = "lookup failed"
			return nil, statusToErrno(status)
		}
		if !proto.IsDir(mode) {
			errmsg = "mode is not dir"
			return nil, syscall.EINVAL
		}
		mp = mw.getPartitionByInode(ctx, inode)
		if mp == nil {
			errmsg = "no such inode"
			return nil, syscall.EAGAIN
		}
		status, info, _, err = mw.iget(ctx, mp, inode, false)
		if err != nil || status != statusOK {
			errmsg = "iget failed"
			return nil, statusToErrno(status)
		}
		if info == nil || info.Nlink > 2 {
			return nil, syscall.ENOTEMPTY
		}
	}

	status, inode, err = mw.ddelete(ctx, parentMP, parentID, name, false)
	if err != nil || status != statusOK {
		if status == statusNoent {
			return nil, nil
		}
		errmsg = "ddelete failed"
		return nil, statusToErrno(status)
	}

	// dentry is deleted successfully but inode is not, still returns success.
	mp = mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		errmsg = "iget failed"
		return nil, nil
	}

	status, info, err = mw.iunlink(ctx, mp, inode, false)
	if err != nil || status != statusOK {
		errmsg = "iunlink failed"
		return nil, nil
	}
	return info, nil
}

func (mw *MetaWrapper) Rename_ll(ctx context.Context, srcParentID uint64, srcName string, dstParentID uint64, dstName string) (deleteIno uint64, err error) {
	var (
		oldInode uint64
		inode    uint64
		mode     uint32
		status   int
		errmsg   string
	)
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("Rename_ll: vol(%v) srcParentID(%v) srcName(%v) srcIno(%v) dstParentID(%v) dstName(%v) dstOldIno(%v) err(%v) errmsg(%v) status(%v)", mw.volname, srcParentID, srcName, inode, dstParentID, dstName, oldInode, err, errmsg, status)
		}
	}()
	srcParentMP := mw.getPartitionByInode(ctx, srcParentID)
	if srcParentMP == nil {
		errmsg = "no srcParentID"
		return 0, syscall.ENOENT
	}
	dstParentMP := mw.getPartitionByInode(ctx, dstParentID)
	if dstParentMP == nil {
		errmsg = "no dstParentID"
		return 0, syscall.ENOENT
	}

	// look up for the src ino
	status, inode, mode, err = mw.lookup(ctx, srcParentMP, srcParentID, srcName)
	if err != nil || status != statusOK {
		errmsg = "no srcName"
		return 0, statusToErrno(status)
	}
	srcMP := mw.getPartitionByInode(ctx, inode)
	if srcMP == nil {
		errmsg = "no srcIno"
		return 0, syscall.ENOENT
	}

	if !proto.IsDbBack {
		status, _, err = mw.ilink(ctx, srcMP, inode)
		if err != nil || status != statusOK {
			errmsg = "ilink srcIno falied"
			return 0, statusToErrno(status)
		}
	}

	// create dentry in dst parent
	status, err = mw.dcreate(ctx, dstParentMP, dstParentID, dstName, inode, mode)
	if err != nil {
		errmsg = "dcreate failed"
		return 0, syscall.EAGAIN
	}

	// Note that only regular files and symbolic links are allowed to be overwritten.
	if status == statusExist && (proto.IsRegular(mode) || proto.IsSymlink(mode)) {
		status, oldInode, err = mw.dupdate(ctx, dstParentMP, dstParentID, dstName, inode)
		if err != nil {
			errmsg = "dupdate failed"
			return 0, syscall.EAGAIN
		}
	}

	if !proto.IsDbBack {
		if status != statusOK {
			mw.iunlink(ctx, srcMP, inode, true)
			errmsg = "iunlink failed"
			return 0, statusToErrno(status)
		}
	}

	// delete dentry from src parent
	status, _, err = mw.ddelete(ctx, srcParentMP, srcParentID, srcName, true)
	// Unable to determin delete dentry status, rollback may cause unexcepted result.
	// So we return error, user should check opration result manually or retry.
	if err != nil || (status != statusOK && status != statusNoent) {
		errmsg = "ddelete failed"
		return 0, statusToErrno(status)
	}

	if !proto.IsDbBack {
		mw.iunlink(ctx, srcMP, inode, true)
	}

	// As update dentry may be try, the second op will be the result which old inode be the same inode
	if oldInode != 0 && oldInode != inode {
		inodeMP := mw.getPartitionByInode(ctx, oldInode)
		if inodeMP != nil {
			mw.iunlink(ctx, inodeMP, oldInode, true)
			deleteIno = oldInode
		}
	}
	log.LogDebugf("Rename_ll: srcParentID:%v srcName:%v dstParentID:%v dstName:%v", srcParentID, srcName, dstParentID, dstName)
	return
}

func (mw *MetaWrapper) ReadDir_ll(ctx context.Context, parentID uint64) (children []proto.Dentry, err error) {
	if mw.VolNotExists() {
		return nil, proto.ErrVolNotExists
	}
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("ReadDir_ll: vol(%v) parentID(%v) err(%v) status(%v)", mw.volname, parentID, err, status)
		}
	}()
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	status, children, _, err = mw.readdir(ctx, parentMP, parentID, "", "", 0)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return children, nil
}

// ReadDir_wo means execute read dir with options.
func (mw *MetaWrapper) ReadDir_wo(parentID uint64, prefix, marker string, count uint64) (children []proto.Dentry, next string, err error) {
	if mw.VolNotExists() {
		return nil, "", proto.ErrVolNotExists
	}
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("ReadDir_wo: vol(%v) parentID(%v) prefix(%v) marker(%v) count(%v) err(%v) status(%v)", mw.volname, parentID, prefix, marker, count, err, status)
		}
	}()
	parentMP := mw.getPartitionByInode(context.Background(), parentID)
	if parentMP == nil {
		return nil, "", syscall.ENOENT
	}

	status, children, next, err = mw.readdir(context.Background(), parentMP, parentID, prefix, marker, count)
	if err != nil || status != statusOK {
		return nil, "", statusToErrno(status)
	}
	return children, next, nil
}

func (mw *MetaWrapper) DentryCreate_ll(ctx context.Context, parentID uint64, name string, inode uint64, mode uint32) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("DentryCreate_ll: vol(%v) parentID(%v) name(%v) ino(%v) mode(%v) err(%v) status(%v)", mw.volname, parentID, name, inode, mode, err, status)
		}
	}()
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return syscall.ENOENT
	}
	if status, err = mw.dcreate(ctx, parentMP, parentID, name, inode, mode); err != nil || status != statusOK {
		err = statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) DentryUpdate_ll(ctx context.Context, parentID uint64, name string, inode uint64) (oldInode uint64, err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("DentryUpdate_ll: vol(%v) parentID(%v) name(%v) ino(%v) err(%v) status(%v)", mw.volname, parentID, name, inode, err, status)
		}
	}()
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		err = syscall.ENOENT
		return
	}
	status, oldInode, err = mw.dupdate(nil, parentMP, parentID, name, inode)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
	}
	return
}

// AppendExtentKeys append multiple extent key into specified inode with single request.
func (mw *MetaWrapper) AppendExtentKeys(ctx context.Context, inode uint64, eks []proto.ExtentKey) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("AppendExtentKeys: vol(%v) ino(%v) eks(%v) err(%v) status(%v)", mw.volname, inode, eks, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err = mw.appendExtentKeys(ctx, mp, inode, eks)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
	}
	log.LogDebugf("AppendExtentKeys: ino(%v) extentKeys(%v)", inode, eks)
	return
}

func (mw *MetaWrapper) InsertExtentKey(ctx context.Context, inode uint64, ek proto.ExtentKey) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("InsertExtentKey: vol(%v) ino(%v) ek(%v) err(%v) status(%v)", mw.volname, inode, ek, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err = mw.insertExtentKey(ctx, mp, inode, ek)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
	}
	log.LogDebugf("InsertExtentKey: ino(%v) ek(%v)", inode, ek)
	return
}

func (mw *MetaWrapper) GetExtents(ctx context.Context, inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("GetExtents: vol(%v) ino(%v) err(%v) status(%v)", mw.volname, inode, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return 0, 0, nil, syscall.ENOENT
	}

	status, gen, size, extents, err = mw.getExtents(ctx, mp, inode)
	if err != nil || status != statusOK {
		return 0, 0, nil, statusToErrno(status)
	}
	log.LogDebugf("GetExtents: ino(%v) gen(%v) size(%v)", inode, gen, size)
	return gen, size, extents, nil
}

func (mw *MetaWrapper) GetExtentsNoModifyAccessTime(ctx context.Context, inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("GetExtentsNoModifyAccessTime: vol(%v) ino(%v) err(%v) status(%v)", mw.volname, inode, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return 0, 0, nil, syscall.ENOENT
	}

	status, gen, size, extents, err = mw.getExtentsNoModifyAccessTime(ctx, mp, inode)
	if err != nil || status != statusOK {
		return 0, 0, nil, statusToErrno(status)
	}
	log.LogDebugf("GetExtentsNoModifyAccessTime: ino(%v) gen(%v) size(%v)", inode, gen, size)
	return gen, size, extents, nil
}

func (mw *MetaWrapper) Truncate(ctx context.Context, inode, oldSize, size uint64) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("Truncate: vol(%v) ino(%v) oldSize(%v) size(%v) err(%v) status(%v)", mw.volname, inode, oldSize, size, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err = mw.truncate(ctx, mp, inode, oldSize, size)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) Link(ctx context.Context, parentID uint64, name string, ino uint64) (info *proto.InodeInfo, err error) {
	var (
		status int
		errmsg string
	)
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogWarnf("Link: vol(%v) parentID(%v) name(%v) ino(%v) err(%v) errmsg(%v) status(%v)", mw.volname, parentID, name, ino, err, errmsg, status)
		}
	}()
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		errmsg = "no parent ino"
		return nil, syscall.ENOENT
	}

	mp := mw.getPartitionByInode(ctx, ino)
	if mp == nil {
		errmsg = "no target ino"
		return nil, syscall.ENOENT
	}

	// increase inode nlink
	status, info, err = mw.ilink(ctx, mp, ino)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	// create new dentry and refer to the inode
	status, err = mw.dcreate(ctx, parentMP, parentID, name, ino, info.Mode)
	if err != nil {
		return nil, statusToErrno(status)
	} else if status != statusOK {
		if status != statusExist {
			mw.iunlink(ctx, mp, ino, true)
		}
		return nil, statusToErrno(status)
	}
	return info, nil
}

func (mw *MetaWrapper) Evict(ctx context.Context, inode uint64, noTrash bool) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogWarnf("Evict: vol(%v) ino(%v) noTrash(%v) err(%v) status(%v)", mw.volname, inode, noTrash, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err = mw.ievict(ctx, mp, inode, noTrash)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) Setattr(ctx context.Context, inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("Setattr: vol(%v) ino(%v) valid(%v) mode(%v) uid(%v) gid(%v) atime(%v) mtime(%v) err(%v) status(%v)", mw.volname, inode, valid, mode, uid, gid, atime, mtime, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err = mw.setattr(ctx, mp, inode, valid, mode, uid, gid, atime, mtime)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) InodeCreate_ll(ctx context.Context, mode, uid, gid uint32, target []byte) (info *proto.InodeInfo, err error) {
	var status int
	defer func() {
		if err != nil {
			log.LogErrorf("InodeCreate_ll: vol(%v) mode(%v) uid(%v) gid(%v) err(%v) status(%v)", mw.volname, mode, uid, gid, err, status)
		}
	}()
	var icreateFunc operatePartitionFunc = func(mp *MetaPartition) (bool, int) {
		status, info, err = mw.icreate(ctx, mp, mode, uid, gid, target)
		return err == nil && status == statusOK, status
	}
	if mw.iteratePartitions(icreateFunc) {
		return info, nil
	}
	return nil, syscall.ENOMEM
}

// InodeLink_ll is a low-level api that makes specified inode link value +1.
func (mw *MetaWrapper) InodeLink_ll(ctx context.Context, inode uint64) (info *proto.InodeInfo, err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("InodeLink_ll: vol(%v) ino(%v) err(%v) status(%v)", mw.volname, inode, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return nil, syscall.ENOENT
	}
	status, info, err = mw.ilink(ctx, mp, inode)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return info, nil
}

// InodeUnlink_ll is a low-level api that makes specified inode link value -1.
func (mw *MetaWrapper) InodeUnlink_ll(ctx context.Context, inode uint64) (info *proto.InodeInfo, err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("InodeUnlink_ll: vol(%v) ino(%v) err(%v) status(%v)", mw.volname, inode, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return nil, syscall.ENOENT
	}
	status, info, err = mw.iunlink(ctx, mp, inode, true)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
		if err == syscall.EAGAIN {
			err = nil
		}
		return nil, err
	}
	return info, nil
}

func (mw *MetaWrapper) InitMultipart_ll(ctx context.Context, path string, extend map[string]string) (multipartId string, err error) {
	var (
		status    int
		sessionId string
	)
	var createMultipartFunc operatePartitionFunc = func(mp *MetaPartition) (bool, int) {
		status, sessionId, err = mw.createMultipart(ctx, mp, path, extend)
		return err == nil && status == statusOK && len(sessionId) > 0, status
	}
	if mw.iteratePartitions(createMultipartFunc) {
		return sessionId, nil
	}
	log.LogErrorf("InitMultipart: create multipart id fail, vol(%v) path(%v) status(%v) err(%v)", mw.volname, path, status, err)
	if err != nil {
		return "", err
	} else {
		return "", statusToErrno(status)
	}
}

func (mw *MetaWrapper) GetMultipart_ll(ctx context.Context, path, multipartId string) (info *proto.MultipartInfo, err error) {
	var (
		mpId  uint64
		found bool
	)
	mpId, found = multipart.MultipartIDFromString(multipartId).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartId, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		info, _, err = mw.broadcastGetMultipart(ctx, path, multipartId)
		return
	}
	var mp = mw.getPartitionByIDWithAutoRefresh(mpId)
	if mp == nil {
		err = syscall.ENOENT
		return
	}
	status, multipartInfo, err := mw.getMultipart(ctx, mp, path, multipartId)
	if err != nil || status != statusOK {
		log.LogErrorf("GetMultipartRequest: vol(%v) err(%v) status(%v)", mw.volname, err, status)
		return nil, statusToErrno(status)
	}
	return multipartInfo, nil
}

func (mw *MetaWrapper) AddMultipartPart_ll(ctx context.Context, path, multipartId string, partId uint16, size uint64, md5 string, inode uint64) (err error) {

	var (
		mpId  uint64
		found bool
	)
	mpId, found = multipart.MultipartIDFromString(multipartId).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartId, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		if _, mpId, err = mw.broadcastGetMultipart(ctx, path, multipartId); err != nil {
			return
		}
	}
	var mp = mw.getPartitionByIDWithAutoRefresh(mpId)
	if mp == nil {
		err = syscall.ENOENT
		return
	}
	status, err := mw.addMultipartPart(ctx, mp, path, multipartId, partId, size, md5, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("AddMultipartPart_ll: vol(%v) err(%v) status(%v)", mw.volname, err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) RemoveMultipart_ll(ctx context.Context, path, multipartID string) (err error) {
	var (
		mpId  uint64
		found bool
	)
	mpId, found = multipart.MultipartIDFromString(multipartID).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartID, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		if _, mpId, err = mw.broadcastGetMultipart(ctx, path, multipartID); err != nil {
			return
		}
	}
	var mp = mw.getPartitionByIDWithAutoRefresh(mpId)
	if mp == nil {
		err = syscall.ENOENT
		return
	}
	status, err := mw.removeMultipart(ctx, mp, path, multipartID)
	if err != nil || status != statusOK {
		log.LogErrorf(" RemoveMultipart_ll: partition remove multipart fail: "+
			"volume(%v) partitionID(%v) multipartID(%v) err(%v) status(%v)",
			mw.volname, mp.PartitionID, multipartID, err, status)
		return statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) broadcastGetMultipart(ctx context.Context, path, multipartId string) (info *proto.MultipartInfo, mpID uint64, err error) {
	log.LogInfof("broadcastGetMultipart: find meta partition broadcast multipartId(%v)", multipartId)
	partitions := mw.GetPartitions()
	var (
		mp *MetaPartition
	)
	var wg = new(sync.WaitGroup)
	var resultMu sync.Mutex
	for _, mp = range partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			status, multipartInfo, err := mw.getMultipart(ctx, mp, path, multipartId)
			if err == nil && status == statusOK && multipartInfo != nil && multipartInfo.ID == multipartId {
				resultMu.Lock()
				mpID = mp.PartitionID
				info = multipartInfo
				resultMu.Unlock()
			}
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("broadcastGetMultipart: get multipart fail: vol(%v) partitionId(%v) multipartId(%v)",
					mw.volname, mp.PartitionID, multipartId)
			}
		}(mp)
	}
	wg.Wait()

	resultMu.Lock()
	defer resultMu.Unlock()
	if info == nil {
		err = syscall.ENOENT
		return
	}
	return
}

func (mw *MetaWrapper) ListMultipart_ll(ctx context.Context, prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (sessionResponse []*proto.MultipartInfo, err error) {
	partitions := mw.GetPartitions()
	var wg = sync.WaitGroup{}
	var wl = sync.Mutex{}
	var sessions = make([]*proto.MultipartInfo, 0)

	for _, mp := range partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			status, response, err := mw.listMultiparts(ctx, mp, prefix, delimiter, keyMarker, multipartIdMarker, maxUploads+1)
			if err != nil || status != statusOK {
				log.LogErrorf("ListMultipart: partition list multipart fail, vol(%v) partitionID(%v) err(%v) status(%v)",
					mw.volname, mp.PartitionID, err, status)
				err = statusToErrno(status)
				return
			}
			wl.Lock()
			defer wl.Unlock()
			sessions = append(sessions, response.Multiparts...)
		}(mp)
	}

	// combine sessions from per partition
	wg.Wait()

	// reorder sessions by path
	sort.SliceStable(sessions, func(i, j int) bool {
		return (sessions[i].Path < sessions[j].Path) || ((sessions[i].Path == sessions[j].Path) && (sessions[i].ID < sessions[j].ID))
	})
	return sessions, nil
}

func (mw *MetaWrapper) XAttrSet_ll(ctx context.Context, inode uint64, name, value []byte) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("XAttrSet_ll: volume(%v) inode(%v) name(%v) value(%v) err(%v) status(%v)", mw.volname, inode, name, value, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}
	status, err = mw.setXAttr(ctx, mp, inode, name, value)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("XAttrSet_ll: volume(%v) inode(%v) name(%v) value(%v) status(%v)",
		mw.volname, inode, name, value, status)
	return
}

func (mw *MetaWrapper) XAttrGet_ll(ctx context.Context, inode uint64, name string) (xAttr *proto.XAttrInfo, err error) {
	var (
		value  string
		status int
	)
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("XAttrGet_ll: volume(%v) inode(%v) name(%v) err(%v) status(%v)", mw.volname, inode, name, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return nil, syscall.ENOENT
	}

	value, status, err = mw.getXAttr(ctx, mp, inode, name)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	xAttrValues := make(map[string]string)
	xAttrValues[name] = string(value)

	xAttr = &proto.XAttrInfo{
		Inode:  inode,
		XAttrs: xAttrValues,
	}

	log.LogDebugf("XAttrGet_ll: get xattr: volume(%v) inode(%v) name(%v) value(%v)",
		mw.volname, inode, string(name), string(value))
	return
}

// XAttrDel_ll is a low-level meta api that deletes specified xattr.
func (mw *MetaWrapper) XAttrDel_ll(ctx context.Context, inode uint64, name string) (err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("XAttrDel_ll: volume(%v) inode(%v) name(%v) err(%v) status(%v)", mw.volname, inode, name, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}
	status, err = mw.removeXAttr(ctx, mp, inode, name)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("XAttrDel_ll: remove xattr, inode(%v) name(%v) status(%v)", inode, name, status)
	return nil
}

func (mw *MetaWrapper) XAttrsList_ll(ctx context.Context, inode uint64) (keys []string, err error) {
	var status int
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("XAttrsList_ll: volume(%v) inode(%v) err(%v) status(%v)", mw.volname, inode, err, status)
		}
	}()
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return nil, syscall.ENOENT
	}
	keys, status, err = mw.listXAttr(ctx, mp, inode)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	return keys, nil
}

func (mw *MetaWrapper) GetInodeExtents_ll(ctx context.Context, mpId uint64, inos []uint64, cnt int, minEkSize int, minInodeSize uint64, maxEkAvgSize uint64) (inodes []*proto.InodeExtents, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetInodeExtents_ll: vol(%v) mpId(%v) inos(%v) cnt(%v) minEkSize(%v) minInodeSize(%v) maxEkAvgSize(%v) err(%v)", mw.volname, mpId, inos, cnt, minEkSize, minInodeSize, maxEkAvgSize, err)
		}
	}()
	mp := mw.getPartitionByID(mpId)
	if mp == nil {
		return nil, fmt.Errorf("no such partition(%v)", mpId)
	}
	return mw.getInodeExtents(ctx, mp, inos, cnt, minEkSize, minInodeSize, maxEkAvgSize)
}

func (mw *MetaWrapper) InodeMergeExtents_ll(ctx context.Context, ino uint64, oldEks []proto.ExtentKey, newEks []proto.ExtentKey, mergeType proto.MergeEkType) (err error) {
	defer func() {
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("InodeMergeExtents_ll: vol(%v) ino(%v) oldEks(%v) newEks(%v) mergeType(%v) err(%v)", mw.volname, ino, oldEks, newEks, mergeType, err)
		}
	}()
	mp := mw.getPartitionByInode(ctx, ino)
	if mp == nil {
		return syscall.ENOENT
	}
	return mw.mergeInodeExtents(ctx, mp, ino, oldEks, newEks, mergeType)
}

func (mw *MetaWrapper) getTargetHosts(ctx context.Context, mp *MetaPartition, members, recorders []string, judgeErrNum int) (targetHosts []string, isErr bool) {
	log.LogDebugf("getTargetHosts because of no leader: mp[%v] members[%v] recorders[%v] judgeErrNum[%v]", mp, members, recorders, judgeErrNum)
	appliedIDslice := make(map[string]uint64, len(members)+len(recorders))
	errSlice := make(map[string]bool)
	isErr = false
	var (
		wg           sync.WaitGroup
		lock         sync.Mutex
		maxAppliedID uint64
	)
	getApplyIDFunc := func(curAddr string, isRecorder bool) {
		appliedID, err := mw.getAppliedID(ctx, mp, curAddr, isRecorder)
		ok := false
		lock.Lock()
		if err != nil {
			errSlice[curAddr] = true
		} else {
			appliedIDslice[curAddr] = appliedID
			ok = true
		}
		lock.Unlock()
		log.LogDebugf("getTargetHosts: get apply id[%v] ok[%v] from host[%v], pid[%v]", appliedID, ok, curAddr, mp.PartitionID)
	}
	for _, addr := range members {
		wg.Add(1)
		go func(curAddr string) {
			getApplyIDFunc(curAddr, false)
			wg.Done()
		}(addr)
	}
	for _, addr := range recorders {
		wg.Add(1)
		go func(curAddr string) {
			getApplyIDFunc(curAddr, true)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	if len(errSlice) >= judgeErrNum {
		isErr = true
		log.LogWarnf("getTargetHosts err: mp[%v], hosts[%v], recorders[%v], appliedID[%v], judgeErrNum[%v]",
			mp.PartitionID, members, recorders, appliedIDslice, judgeErrNum)
		return
	}
	targetHosts, maxAppliedID = getMaxApplyIDHosts(appliedIDslice, recorders)
	if len(targetHosts) == 0 {
		log.LogWarnf("mp[%v] no target hosts: applyIDMap[%v] recorders[%v]", mp.PartitionID, appliedIDslice, recorders)
	}
	log.LogDebugf("getTargetHosts: get max apply id[%v] from hosts[%v], pid[%v]", maxAppliedID, targetHosts, mp.PartitionID)
	return targetHosts, isErr
}

func ExcludeLearner(mp *MetaPartition) (members []string) {
	members = make([]string, 0)
	for _, host := range mp.Members {
		if !contains(mp.Learners, host) {
			members = append(members, host)
		}
	}
	return members
}
func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}
	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}

func getMaxApplyIDHosts(appliedIDslice map[string]uint64, recorders []string) (targetHosts []string, maxID uint64) {
	maxID = uint64(0)
	targetHosts = make([]string, 0)
	for _, id := range appliedIDslice {
		if id >= maxID {
			maxID = id
		}
	}
	for addr, id := range appliedIDslice {
		if id == maxID && !contains(recorders, addr) {
			targetHosts = append(targetHosts, addr)
		}
	}
	return
}

func (mw *MetaWrapper) LookupDeleted_ll(ctx context.Context, parentID uint64, name string, startTime, endTime int64) (
	dentrys []*proto.DeletedDentry, err error) {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		log.LogErrorf("LookupDeleted_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	var status int
	status, err, dentrys = mw.lookupDeleted(ctx, parentMP, parentID, name, startTime, endTime)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) ReadDirDeleted(ctx context.Context, parentID uint64) ([]*proto.DeletedDentry, error) {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	var (
		name      string
		timestamp int64
	)

	res := make([]*proto.DeletedDentry, 0)
	for {
		status, children, err := mw.readDeletedDir(ctx, parentMP, parentID, name, timestamp)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		for _, child := range children {
			res = append(res, child)
		}
		if len(children) < proto.ReadDeletedDirBatchNum {
			break
		}
		name = children[proto.ReadDeletedDirBatchNum-1].Name
		timestamp = children[proto.ReadDeletedDirBatchNum-1].Timestamp
	}

	return res, nil
}

func (mw *MetaWrapper) RecoverDeletedDentry(ctx context.Context, parentID, inodeID uint64, name string, timestamp int64) error {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return syscall.ENOENT
	}

	status, err := mw.recoverDentry(ctx, parentMP, parentID, inodeID, name, timestamp)
	if err != nil {
		return err
	}
	if status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) BatchRecoverDeletedDentry(ctx context.Context, dens []*proto.DeletedDentry) (
	res map[uint64]int, err error) {
	if dens == nil || len(dens) == 0 {
		err = errors.New("BatchRecoverDeletedDentry, the dens is 0.")
		return
	}

	log.LogDebugf("BatchRecoverDeletedDentry: len(dens): %v", len(dens))
	for index, den := range dens {
		log.LogDebugf("index: %v, den: %v", index, den)
	}
	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchOpDeletedDentryRsp, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]*proto.DeletedDentry)

	// Target partition does not have to be very accurate.
	for _, den := range dens {
		mp := mw.getPartitionByInode(ctx, den.ParentID)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]*proto.DeletedDentry, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], den)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchRecoverDeletedDentry(ctx, &wg, mp, inos, batchChan)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, den := range infos.Dens {
			res[den.Den.Inode] = parseStatus(den.Status)
		}
	}
	return

}

func (mw *MetaWrapper) RecoverDeletedInode(ctx context.Context, inodeID uint64) error {
	mp := mw.getPartitionByInode(ctx, inodeID)
	if mp == nil {
		log.LogErrorf("RecoverDeleteInode: No such partition, ino(%v)", inodeID)
		return syscall.ENOENT
	}

	status, err := mw.recoverDeletedInode(ctx, mp, inodeID)
	if err != nil {
		return err
	}
	if status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) BatchRecoverDeletedInode(ctx context.Context, inodes []uint64) (res map[uint64]int, err error) {
	log.LogDebugf("BatchRecoverDeletedInode: len(dens): %v", len(inodes))

	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchOpDeletedINodeRsp, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]uint64)

	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchRecoverDeletedInode(ctx, &wg, mp, inos, batchChan)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, ino := range infos.Inos {
			res[ino.Inode.Inode] = parseStatus(ino.Status)
		}
	}

	return
}

func (mw *MetaWrapper) BatchCleanDeletedDentry(ctx context.Context, dens []*proto.DeletedDentry) (res map[uint64]int, err error) {
	if dens == nil || len(dens) == 0 {
		err = errors.New("BatchCleanDeletedDentry, the dens is 0.")
		return
	}

	log.LogDebugf("BatchCleanDeletedDentry: len(dens): %v", len(dens))
	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchOpDeletedDentryRsp, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]*proto.DeletedDentry)

	for _, den := range dens {
		mp := mw.getPartitionByInode(ctx, den.ParentID)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]*proto.DeletedDentry, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], den)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchCleanDeletedDentry(ctx, &wg, mp, inos, batchChan)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, d := range infos.Dens {
			res[d.Den.Inode] = parseStatus(d.Status)
		}
	}
	return
}

func (mw *MetaWrapper) CleanDeletedDentry(ctx context.Context, parentID, inodeID uint64, name string, timestamp int64) error {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return syscall.ENOENT
	}

	status, err := mw.cleanDeletedDentry(ctx, parentMP, parentID, inodeID, name, timestamp)
	if err != nil {
		return err
	}
	if status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) BatchCleanDeletedInode(ctx context.Context, inodes []uint64) (res map[uint64]int, err error) {
	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchOpDeletedINodeRsp, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]uint64)

	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchCleanDeletedInode(ctx, &wg, mp, inos, batchChan)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, ino := range infos.Inos {
			res[ino.Inode.Inode] = parseStatus(ino.Status)
		}
	}

	return
}

func (mw *MetaWrapper) CleanDeletedInode(ctx context.Context, inodeID uint64) error {
	mp := mw.getPartitionByInode(ctx, inodeID)
	if mp == nil {
		log.LogErrorf("RecoverDeleteInode: No such partition, ino(%v)", inodeID)
		return syscall.ENOENT
	}

	status, err := mw.cleanDeletedInode(ctx, mp, inodeID)
	if err != nil {
		return err
	}
	if status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) GetDeletedInode(ctx context.Context, inode uint64) (*proto.DeletedInodeInfo, error) {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("GetDeletedInode: No such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.getDeletedInodeInfo(ctx, mp, inode)
	if err != nil {
		return nil, err
	}
	if status != statusOK {
		err = statusToErrno(status)
		return nil, err
	}
	log.LogDebugf("GetDeletedInode: info(%v)", info)
	return info, nil
}

func (mw *MetaWrapper) BatchGetDeletedInode(ctx context.Context, inodes []uint64) map[uint64]*proto.DeletedInodeInfo {
	var wg sync.WaitGroup
	batchInfos := make(map[uint64]*proto.DeletedInodeInfo, 0)
	resp := make(chan []*proto.DeletedInodeInfo, BatchIgetRespBuf)
	candidates := make(map[uint64][]uint64)

	// Target partition does not have to be very accurate.
	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchGetDeletedInodeInfo(ctx, &wg, mp, inos, resp)
	}

	go func() {
		wg.Wait()
		close(resp)
	}()

	for infos := range resp {
		for _, di := range infos {
			batchInfos[di.Inode] = di
		}
	}
	return batchInfos
}

func (mw *MetaWrapper) StatDeletedFileInfo(ctx context.Context, pid uint64) (resp *proto.StatDeletedFileInfoResponse, err error) {
	mp := mw.getPartitionByID(pid)
	if mp == nil {
		log.LogErrorf("StatDeletedFileInfo: No such partition, pid(%v)", pid)
		return nil, syscall.ENOENT
	}

	status := statusOK
	resp, status, err = mw.statDeletedFileInfo(ctx, mp)
	if err != nil {
		return
	}
	if status != statusOK {
		err = statusToErrno(status)
		return
	}
	return
}

func (mw *MetaWrapper) CleanExpiredDeletedDentry(ctx context.Context, pid uint64, deadline uint64) (err error) {
	mp := mw.getPartitionByID(pid)
	if mp == nil {
		log.LogErrorf("CleanExpiredDeletedDentry: No such partition, pid(%v)", pid)
		return syscall.ENOENT
	}

	st := statusOK
	st, err = mw.cleanExpiredDeletedDentry(ctx, mp, deadline)
	if err != nil {
		return
	}
	if st != statusOK {
		err = statusToErrno(st)
		return
	}
	return
}

func (mw *MetaWrapper) CleanExpiredDeletedInode(ctx context.Context, pid uint64, deadline uint64) (err error) {
	mp := mw.getPartitionByID(pid)
	if mp == nil {
		log.LogErrorf("CleanExpiredDeletedInode: No such partition, pid(%v)", pid)
		return syscall.ENOENT
	}

	st := statusOK
	st, err = mw.cleanExpiredDeletedInode(ctx, mp, deadline)
	if err != nil {
		log.LogErrorf("CleanExpiredDeletedInode, err: %v", err.Error())
		return
	}
	if st != statusOK {
		err = statusToErrno(st)
		return
	}
	return
}

func (mw *MetaWrapper) BatchUnlinkInodeUntest(ctx context.Context, inodes []uint64, noTrash bool) (res map[uint64]int, err error) {
	log.LogDebugf("BatchUnlinkInodeUntest: len(dens): %v", len(inodes))
	for index, ino := range inodes {
		log.LogDebugf("index: %v, den: %v", index, ino)
	}

	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchUnlinkInodeResponse, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]uint64)

	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchUnlinkInodeUntest(ctx, &wg, mp, inos, batchChan, noTrash)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, it := range infos.Items {
			res[it.Info.Inode] = parseStatus(it.Status)
		}
	}

	for id := range res {
		val, _ := res[id]
		log.LogDebugf("==> BatchUnlinkInodeUntest: ino: %v, status: %v", id, val)
	}
	return
}

func (mw *MetaWrapper) BatchEvictInodeUntest(ctx context.Context, inodes []uint64, noTrash bool) (res map[uint64]int, err error) {
	log.LogDebugf("BatchEvictInodeUntest: len(dens): %v", len(inodes))
	for index, ino := range inodes {
		log.LogDebugf("index: %v, den: %v", index, ino)
	}

	var wg sync.WaitGroup
	batchChan := make(chan int, len(inodes))
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]uint64)

	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchEvictInodeUntest(ctx, &wg, mp, inos, batchChan, noTrash)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for st := range batchChan {
		if st != statusOK {
			err = fmt.Errorf(" status: %v", st)
			break
		}
	}

	return
}

func (mw *MetaWrapper) BatchDeleteDentryUntest(ctx context.Context, pid uint64, dens []proto.Dentry, noTrash bool) (
	res map[uint64]int, err error) {
	if dens == nil || len(dens) == 0 {
		err = errors.New("BatchDeleteDentryUntest, the dens is 0.")
		return
	}

	log.LogDebugf("BatchDeleteDentryUntest: len(dens): %v", len(dens))
	for index, den := range dens {
		log.LogDebugf("index: %v, den: %v", index, den)
	}
	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchDeleteDentryResponse, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]proto.Dentry)

	// Target partition does not have to be very accurate.
	for _, den := range dens {
		mp := mw.getPartitionByInode(ctx, pid)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]proto.Dentry, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], den)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchDeleteDentryUntest(ctx, &wg, mp, pid, inos, batchChan, noTrash)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, it := range infos.Items {
			res[it.Inode] = parseStatus(it.Status)
		}
	}
	return
}
