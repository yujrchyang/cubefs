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

package fs

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/net/context"
)

// Create handles the create request.
func (d *Node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	var err error
	tpObject := exporter.NewModuleTPUs("filecreate_us")
	defer tpObject.Set(err)

	start := time.Now()
	if !d.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
		return nil, nil, fuse.EPERM
	}
	info, err := Sup.mw.Create_ll(ctx, d.inode, req.Name, proto.Mode(req.Mode.Perm()), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Create: parent(%v) req(%v) err(%v)", d.inode, req, err)
		return nil, nil, ParseError(err)
	}

	Sup.ic.Put(info)
	if Sup.Flock() {
		Sup.ic.PutParent(info.Inode, d.inode)
	}
	child := NewNode(info.Inode)
	Sup.ec.OpenStream(info.Inode, false)

	if Sup.keepCache {
		resp.Flags |= fuse.OpenKeepCache
	}
	resp.EntryValid = LookupValidDuration

	Sup.ic.Delete(ctx, d.inode)

	log.LogDebugf("TRACE Create: parent(%v) req(%v) resp(%v) ino(%v) time(%v)", d.inode, req, resp, info.Inode, time.Since(start))
	return child, child, nil
}

// Mkdir handles the mkdir request.
func (d *Node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	var err error
	tpObject := exporter.NewModuleTPUs("mkdir_us")
	defer tpObject.Set(err)

	start := time.Now()
	if !d.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
		return nil, fuse.EPERM
	}
	info, err := Sup.mw.Create_ll(ctx, d.inode, req.Name, proto.Mode(os.ModeDir|req.Mode.Perm()), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Mkdir: parent(%v) req(%v) err(%v)", d.inode, req, err)
		return nil, ParseError(err)
	}

	Sup.ic.Put(info)
	if Sup.Flock() {
		Sup.ic.PutParent(info.Inode, d.inode)
	}
	child := NewNode(info.Inode)
	Sup.ic.Delete(ctx, d.inode)

	log.LogDebugf("TRACE Mkdir: parent(%v) req(%v) ino(%v) time(%v)", d.inode, req, info.Inode, time.Since(start))
	return child, nil
}

// Remove handles the remove request.
func (d *Node) Remove(ctx context.Context, req *fuse.RemoveRequest) (err error) {
	tpObject := exporter.NewVolumeTPUs("Remove_us", Sup.volname)
	tpObject1 := exporter.NewModuleTPUs("remove_us")
	defer func() {
		tpObject.Set(err)
		tpObject1.Set(err)
	}()

	start := time.Now()
	if len(Sup.delProcessPath) > 0 {
		delProcPath, errStat := os.Readlink(fmt.Sprintf("/proc/%v/exe", req.Pid))
		if errStat != nil || !contains(Sup.delProcessPath, delProcPath) {
			log.LogErrorf("Remove: pid(%v) process(%v) is not permitted err(%v), parent(%v) name(%v)", req.Pid, delProcPath, errStat, d.inode, req.Name)
			return fuse.EPERM
		}
		log.LogDebugf("Remove: allow process pid(%v) path(%v) to delete file, parent(%v) name(%v)", req.Pid, delProcPath, d.inode, req.Name)
	}
	if !d.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
		return fuse.EPERM
	}

	// dbbak doesn't maitain nlink in directory inode, shouldn't rely on it when deleting
	if req.Dir && proto.IsDbBack {
		var err2 error
		target, ok := d.dcache.Get(req.Name)
		if !ok {
			target, _, err2 = Sup.mw.Lookup_ll(ctx, d.inode, req.Name)
			if err2 != nil {
				if err2 != syscall.ENOENT {
					msg := fmt.Sprintf("Remove: parent(%v) name(%v) err(%v)", d.inode, req.Name, err2)
					log.LogErrorf(msg)
					Sup.handleError("Remove", msg)
				}
				err = ParseError(err2)
				return
			}
		}

		children, err2 := Sup.mw.ReadDir_ll(ctx, target)
		if err2 != nil {
			msg := fmt.Sprintf("Remove: readdir failed, parent(%v), name(%v), err(%v)", d.inode, req.Name, err2)
			log.LogErrorf(msg)
			if err2 != syscall.ENOENT {
				Sup.handleError("Remove", msg)
			}
			err = ParseError(err2)
			return
		}

		if len(children) != 0 {
			log.LogWarnf("Remove: dir not empty, parent(%v), name(%v), ino(%v), numOfChildren(%v)", d.inode, req.Name, target, len(children))
			err = ParseError(syscall.ENOTEMPTY)
			return
		}
	}

	d.dcache.Delete(req.Name)
	info, syserr := Sup.mw.Delete_ll(ctx, d.inode, req.Name, req.Dir)
	if syserr != nil {
		log.LogErrorf("Remove: parent(%v) name(%v) err(%v)", d.inode, req.Name, syserr)
		if syserr == syscall.EIO {
			msg := fmt.Sprintf("parent(%v) name(%v) err(%v)", d.inode, req.Name, syserr)
			Sup.handleError("Remove", msg)
		}
		err = ParseError(syserr)
		return
	}

	Sup.ic.Delete(ctx, d.inode)
	if info != nil {
		if proto.IsDir(info.Mode) {
			Sup.ic.DeleteParent(info.Inode)
		}
		if info.Nlink == 0 && !proto.IsDir(info.Mode) {
			Sup.orphan.Put(info.Inode)
			log.LogDebugf("Remove: add to orphan inode list, ino(%v)", info.Inode)
		}
	}

	log.LogDebugf("TRACE Remove: parent(%v) req(%v) inode(%v) time(%v)", d.inode, req, info, time.Since(start))
	return nil
}

// Lookup handles the lookup request.
func (d *Node) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	var (
		ino uint64
		err error
	)

	tpObject := exporter.NewVolumeTPUs("Lookup_us", Sup.volname)
	defer func() {
		tpObject.Set(err)
	}()
	start := time.Now()
	if log.IsDebugEnabled() {
		log.LogDebugf("TRACE Lookup enter: parent(%v) req(%v)", d.inode, req)
	}

	ino, _, ok := d.dcache.Get(req.Name)
	if !ok && Sup.prefetchManager != nil {
		dcache := Sup.prefetchManager.GetDentryCache(d.inode)
		if dcache != nil {
			d.dcache = dcache
			ino, _, ok = d.dcache.Get(req.Name)
		}
	}
	if !ok {
		ino, _, err = Sup.mw.Lookup_ll(ctx, d.inode, req.Name)
		if err != nil {
			if err != syscall.ENOENT {
				log.LogErrorf("Lookup: parent(%v) name(%v) err(%v)", d.inode, req.Name, err)
			}
			return nil, ParseError(err)
		}
	}

	child := NewNode(ino)
	if Sup.Flock() {
		Sup.ic.PutParent(ino, d.inode)
	}
	resp.EntryValid = LookupValidDuration

	if log.IsDebugEnabled() {
		log.LogDebugf("TRACE Lookup: parent(%v) req(%v) cost(%v)", d.inode, req, time.Since(start))
	}
	return child, nil
}

// ReadDirAll gets all the dentries in a directory and puts them into the cache.
func (d *Node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTPUs("readdir_us")
	defer tpObject.Set(err)

	children, err := Sup.mw.ReadDir_ll(ctx, d.inode)
	if err != nil {
		log.LogErrorf("Readdir: ino(%v) err(%v)", d.inode, err)
		return make([]fuse.Dirent, 0), ParseError(err)
	}

	inodes := make([]uint64, 0, len(children))
	dirents := make([]fuse.Dirent, 0, len(children))

	var dcache *cache.DentryCache
	if !Sup.disableDcache {
		dcache = cache.NewDentryCache(DentryValidSec, true)
	}

	for _, child := range children {
		dentry := fuse.Dirent{
			Inode: child.Inode,
			Type:  ParseType(child.Type),
			Name:  child.Name,
		}
		inodes = append(inodes, child.Inode)
		dirents = append(dirents, dentry)
		dcache.Put(child.Name, child.Inode, child.Type)
	}

	// batch get inode info is only useful when using stat/fstat to all files, or in shell ls command
	if !Sup.noBatchGetInodeOnReaddir {
		infos := Sup.mw.BatchInodeGet(ctx, inodes)
		for _, info := range infos {
			Sup.ic.Put(info)
			if Sup.Flock() {
				Sup.ic.PutParent(info.Inode, d.inode)
			}
		}
	}
	d.dcache = dcache

	log.LogDebugf("TRACE ReadDir: ino(%v) children count(%v) time(%v)", d.inode, len(children), time.Since(start))
	return dirents, nil
}

// ReadDirPlusAll gets all the dentries and their information in a directory and puts them into the cache.
func (d *Node) ReadDirPlusAll(ctx context.Context, resp *fuse.ReadDirPlusResponse) ([]*fs.DirentPlus, error) {
	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTPUs("readdirplus_us")
	defer tpObject.Set(err)

	children, err := Sup.mw.ReadDir_ll(ctx, d.inode)
	if err != nil {
		log.LogErrorf("ReaddirPlus: ino(%v) err(%v)", d.inode, err)
		return make([]*fs.DirentPlus, 0), ParseError(err)
	}

	inodes := make([]uint64, 0, len(children))
	for _, child := range children {
		inodes = append(inodes, child.Inode)
	}
	infos := Sup.mw.BatchInodeGet(ctx, inodes)
	infoMap := make(map[uint64]*proto.InodeInfo, len(infos))
	for _, info := range infos {
		Sup.ic.Put(info)
		if Sup.Flock() {
			Sup.ic.PutParent(info.Inode, d.inode)
		}
		infoMap[info.Inode] = info
	}

	var dcache *cache.DentryCache
	if !Sup.disableDcache {
		dcache = cache.NewDentryCache(DentryValidSec, true)
	}
	dirents := make([]*fs.DirentPlus, 0, len(children))
	for _, child := range children {
		dentryPlus := &fs.DirentPlus{}
		dentryPlus.Dirent = fuse.Dirent{
			Inode: child.Inode,
			Type:  ParseType(child.Type),
			Name:  child.Name,
		}
		info, exist := infoMap[child.Inode]
		if exist {
			dentryPlus.Node = NewNode(info.Inode)
		}
		dirents = append(dirents, dentryPlus)
		dcache.Put(child.Name, child.Inode, child.Type)
	}

	d.dcache = dcache
	resp.EntryValid = LookupValidDuration

	log.LogDebugf("TRACE ReadDirPlus: ino(%v) resp(%v) children count(%v) time(%v)", d.inode, resp, len(dirents), time.Since(start))
	return dirents, nil
}

// Rename handles the rename request.
func (d *Node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	var destDirIno uint64
	if Sup.notCacheNode {
		destDirIno = uint64(req.NewDir)
	} else {
		dstDir, ok := newDir.(*Node)
		if !ok {
			log.LogErrorf("Rename: NOT DIR, parent(%v) req(%v)", d.inode, req)
			return fuse.ENOTSUP
		}
		destDirIno = dstDir.Inode()
	}
	start := time.Now()
	d.dcache.Delete(req.OldName)

	var err error
	tpObject := exporter.NewModuleTPUs("rename_us")
	defer tpObject.Set(err)

	if !d.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
		return fuse.EPERM
	}
	err = Sup.mw.Rename_ll(ctx, d.inode, req.OldName, destDirIno, req.NewName, false)
	if err != nil {
		log.LogErrorf("Rename: parent(%v) req(%v) err(%v)", d.inode, req, err)
		return ParseError(err)
	}

	Sup.ic.Delete(ctx, d.inode)
	Sup.ic.Delete(ctx, destDirIno)

	log.LogDebugf("TRACE Rename: SrcParent(%v) OldName(%v) DstParent(%v) NewName(%v) time(%v)", d.inode, req.OldName, destDirIno, req.NewName, time.Since(start))
	return nil
}

func (d *Node) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {
	if (req.Mode&os.ModeNamedPipe == 0 && req.Mode&os.ModeSocket == 0) || req.Rdev != 0 {
		return nil, fuse.ENOSYS
	}

	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTPUs("mknod_us")
	defer tpObject.Set(err)

	if !d.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
		return nil, fuse.EPERM
	}
	info, err := Sup.mw.Create_ll(ctx, d.inode, req.Name, proto.Mode(req.Mode), req.Uid, req.Gid, nil)
	if err != nil {
		log.LogErrorf("Mknod: parent(%v) req(%v) err(%v)", d.inode, req, err)
		return nil, ParseError(err)
	}

	Sup.ic.Put(info)
	if Sup.Flock() {
		Sup.ic.PutParent(info.Inode, d.inode)
	}
	child := NewNode(info.Inode)

	log.LogDebugf("TRACE Mknod: parent(%v) req(%v) ino(%v) time(%v)", d.inode, req, info.Inode, time.Since(start))
	return child, nil
}

// Symlink handles the symlink request.
func (d *Node) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	parentIno := d.inode
	start := time.Now()

	var err error
	tpObject := exporter.NewModuleTPUs("symlink_us")
	defer tpObject.Set(err)

	if !d.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
		return nil, fuse.EPERM
	}
	info, err := Sup.mw.Create_ll(ctx, parentIno, req.NewName, proto.Mode(os.ModeSymlink|os.ModePerm), req.Uid, req.Gid, []byte(req.Target))
	if err != nil {
		log.LogErrorf("Symlink: parent(%v) NewName(%v) err(%v)", parentIno, req.NewName, err)
		return nil, ParseError(err)
	}

	Sup.ic.Put(info)
	child := NewNode(info.Inode)

	log.LogDebugf("TRACE Symlink: parent(%v) req(%v) ino(%v) time(%v)", parentIno, req, info.Inode, time.Since(start))
	return child, nil
}

// Link handles the link request.
func (d *Node) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	var oldInode uint64
	if Sup.notCacheNode {
		oldInode = uint64(req.OldNode)
	} else {
		switch old := old.(type) {
		case *Node:
			oldInode = old.inode
		default:
			return nil, fuse.EPERM
		}
	}

	var err error
	tpObject := exporter.NewModuleTPUs("link_us")
	defer tpObject.Set(err)

	start := time.Now()
	if !d.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
		return nil, fuse.EPERM
	}
	info, err := Sup.mw.Link(ctx, d.inode, req.NewName, oldInode)
	if err != nil {
		log.LogErrorf("Link: parent(%v) name(%v) ino(%v) err(%v)", d.inode, req.NewName, oldInode, err)
		return nil, ParseError(err)
	}

	Sup.ic.Put(info)
	newFile := NewNode(info.Inode)
	log.LogDebugf("TRACE Link: parent(%v) name(%v) ino(%v) time(%v)", d.inode, req.NewName, info.Inode, time.Since(start))
	return newFile, nil
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
