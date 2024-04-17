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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/net/context"
)

// Node defines the structure of a node.
type Node struct {
	inode  uint64
	dcache *cache.DentryCache
}

// NewNode returns a new node.
func NewNode(inode uint64) fs.Node {
	return &Node{inode: inode}
}

// Getattr gets the attributes of a file.
func (f *Node) Attr(ctx context.Context, a *fuse.Attr) error {
	ino := f.inode
	info, err := Sup.InodeGet(ctx, ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", ino, err)
		if err == fuse.ENOENT {
			a.Inode = ino
			return nil
		}
		return ParseError(err)
	}

	fillAttr(info, a)
	if proto.IsRegular(info.Mode) {
		fileSize, gen := f.fileSize(ctx, ino)
		if gen >= info.Generation {
			a.Size = uint64(fileSize)
		}
	} else if proto.IsSymlink(info.Mode) && info.Target != nil {
		a.Size = uint64(len(*info.Target))
	}

	log.LogDebugf("TRACE Attr: inode(%v) attr(%v)", info, a)
	return nil
}

func (f *Node) Inode() uint64 {
	return f.inode
}

// Forget evicts the inode of the current file. This can only happen when the inode is on the orphan list.
func (f *Node) Forget() {
	ino := f.inode
	defer func() {
		log.LogDebugf("TRACE Forget: ino(%v)", ino)
	}()

	Sup.ic.Delete(nil, ino)
	if err := Sup.ec.EvictStream(nil, ino); err != nil {
		log.LogWarnf("Forget: stream not ready to evict, ino(%v) err(%v)", ino, err)
		return
	}

	if Sup.orphan.Evict(ino) {
		if err := Sup.mw.Evict(nil, ino, false); err != nil {
			log.LogWarnf("Forget Evict: ino(%v) err(%v)", ino, err)
		}
	}
	Sup.ic.DeleteParent(ino)
}

// Open handles the open request.
func (f *Node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (handle fs.Handle, err error) {
	if req != nil && req.Dir {
		return f, nil
	}

	ino := f.inode
	// open from rebuildFuseContext doesn't have req&resp
	if req == nil {
		var info *proto.InodeInfo
		if info, err = Sup.InodeGet(ctx, ino); err != nil {
			return
		}
		if proto.IsDir(info.Mode) {
			return f, nil
		}
	}

	tpObject := exporter.NewVolumeTPUs("Open_us", Sup.volname)
	tpObject1 := exporter.NewModuleTP("fileopen")
	defer func() {
		tpObject.Set(err)
		tpObject1.Set(err)
	}()

	start := time.Now()
	Sup.ec.OpenStream(ino, false)
	if Sup.prefetchManager == nil {
		Sup.ec.RefreshExtentsCache(ctx, ino)
	}

	if Sup.keepCache && resp != nil {
		resp.Flags |= fuse.OpenKeepCache
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("TRACE Open: ino(%v) req(%v) resp(%v) (%v)", ino, req, resp, time.Since(start))
	}
	return f, nil
}

// Release handles the release request.
func (f *Node) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	if req.Dir {
		return
	}
	tpObject := exporter.NewVolumeTPUs("Release_us", Sup.volname)
	defer func() {
		tpObject.Set(err)
	}()

	ino := f.inode
	if log.IsDebugEnabled() {
		log.LogDebugf("TRACE Release enter: ino(%v) req(%v)", ino, req)
	}

	start := time.Now()

	//log.LogDebugf("TRACE Release close stream: ino(%v) req(%v)", ino, req)

	err = Sup.ec.CloseStream(ctx, ino)
	if err != nil {
		log.LogErrorf("Release: close writer failed, ino(%v) req(%v) err(%v)", ino, req, err)
		return fuse.EIO
	}

	if Sup.prefetchManager == nil {
		Sup.ic.Delete(ctx, ino)
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("TRACE Release: ino(%v) req(%v) (%v)", ino, req, time.Since(start))
	}
	return nil
}

// Read handles the read request.
func (f *Node) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	tpObject := exporter.NewVolumeTPUs("Read_us", Sup.volname)
	tpObject1 := exporter.NewModuleTP("fileread")
	defer func() {
		tpObject.Set(err)
		tpObject1.Set(err)
	}()
	Sup.prefetchManager.AddTotalReadCount()
	if Sup.prefetchManager.ContainsAppPid(req.Pid) {
		Sup.prefetchManager.AddAppReadCount()
		tpObjectPid := exporter.NewVolumeTPUs("AppRead_us", Sup.volname)
		defer func() {
			tpObjectPid.Set(err)
			log.LogWarnf("Read CFS: ino(%v) offset(%v) reqsize(%v) req(%v)", f.inode, req.Offset, req.Size, req)
		}()
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("TRACE Read enter: ino(%v) offset(%v) reqsize(%v) req(%v)", f.inode, req.Offset, req.Size, req)
	}

	start := time.Now()

	size, _, err := Sup.ec.Read(ctx, f.inode, resp.Data[fuse.OutHeaderSize:(fuse.OutHeaderSize+req.Size)], uint64(req.Offset), req.Size)
	if err != nil && err != io.EOF {
		msg := fmt.Sprintf("Read: ino(%v) req(%v) err(%v) size(%v)", f.inode, req, err, size)
		Sup.handleErrorWithGetInode("Read", msg, f.inode)
		return fuse.EIO
	}

	if size > req.Size {
		msg := fmt.Sprintf("Read: read size larger than request size, ino(%v) req(%v) size(%v)", f.inode, req, size)
		Sup.handleError("Read", msg)
		return fuse.ERANGE
	}

	if size > 0 {
		resp.ActualSize = uint64(size + fuse.OutHeaderSize)
	} else if size <= 0 {
		resp.ActualSize = uint64(fuse.OutHeaderSize)
		log.LogWarnf("Read: ino(%v) offset(%v) reqsize(%v) req(%v) size(%v)", f.inode, req.Offset, req.Size, req, size)
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("TRACE Read: ino(%v) offset(%v) reqsize(%v) req(%v) size(%v) (%v)", f.inode, req.Offset, req.Size, req, size, time.Since(start))
	}
	return nil
}

// Write handles the write request.
func (f *Node) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	var (
		ino         = f.inode
		filesize    uint64
		newFileSize uint64
	)
	tpObject := exporter.NewVolumeTPUs("Write_us", Sup.volname)
	tpObject1 := exporter.NewModuleTP("filewrite")
	defer func() {
		tpObject.Set(err)
		tpObject1.Set(err)
		newFileSize, _ = f.fileSize(ctx, ino)
		if newFileSize > filesize {
			info := Sup.ic.Get(ctx, ino)
			if info != nil {
				info.Size = newFileSize
			}
		}
	}()

	if !f.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
		return fuse.EPERM
	}

	reqlen := len(req.Data)
	filesize, _ = f.fileSize(ctx, ino)

	log.LogDebugf("TRACE Write enter: ino(%v) offset(%v) len(%v) filesize(%v) flags(%v) fileflags(%v) req(%v)", ino, req.Offset, reqlen, filesize, req.Flags, req.FileFlags, req)

	if !proto.IsDbBack && req.Offset > int64(filesize) && reqlen == 1 && req.Data[0] == 0 {
		// workaround: posix_fallocate would write 1 byte if fallocate is not supported.
		err = Sup.ec.Truncate(ctx, ino, filesize, uint64(req.Offset)+uint64(reqlen))
		if err == nil {
			resp.Size = reqlen
		}

		log.LogDebugf("fallocate: ino(%v) origFilesize(%v) req(%v) err(%v)", f.inode, filesize, req, err)
		return
	}

	var waitForFlush, enSyncWrite bool
	if isDirectIOEnabled(req.FileFlags) || (req.FileFlags&fuse.OpenSync != 0) {
		waitForFlush = true
	}
	enSyncWrite = Sup.enSyncWrite
	start := time.Now()

	offset := uint64(req.Offset)
	if proto.IsDbBack {
		offset = filesize
	}
	size, _, err := Sup.ec.Write(ctx, ino, offset, req.Data, enSyncWrite)
	if err != nil {
		msg := fmt.Sprintf("Write: ino(%v) offset(%v) len(%v) err(%v)", ino, req.Offset, reqlen, err)
		Sup.handleErrorWithGetInode("Write", msg, ino)
		return fuse.EIO
	}

	resp.Size = size
	if size != reqlen {
		log.LogErrorf("Write: ino(%v) offset(%v) len(%v) size(%v)", ino, req.Offset, reqlen, size)
	}

	if waitForFlush {
		if err = Sup.ec.Flush(ctx, ino); err != nil {
			msg := fmt.Sprintf("Write: failed to wait for flush, ino(%v) offset(%v) len(%v) err(%v) req(%v)", ino, req.Offset, reqlen, err, req)
			Sup.handleErrorWithGetInode("Wrtie", msg, ino)
			return fuse.EIO
		}
	}

	log.LogDebugf("TRACE Write: ino(%v) offset(%v) len(%v) flags(%v) fileflags(%v) req(%v) (%v)",
		ino, req.Offset, reqlen, req.Flags, req.FileFlags, req, time.Since(start))
	return nil
}

// Flush only when fsyncOnClose is enabled.
func (f *Node) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	if !Sup.fsyncOnClose {
		return fuse.ENOSYS
	}
	log.LogDebugf("TRACE Flush enter: ino(%v)", f.inode)
	start := time.Now()

	tpObject := exporter.NewVolumeTPUs("Flush_us", Sup.volname)
	tpObject1 := exporter.NewModuleTP("filesync")
	defer func() {
		tpObject.Set(err)
		tpObject1.Set(err)
	}()

	err = Sup.ec.Flush(ctx, f.inode)
	if err != nil {
		msg := fmt.Sprintf("Flush: ino(%v) err(%v)", f.inode, err)
		Sup.handleErrorWithGetInode("Flush", msg, f.inode)
		return fuse.EIO
	}
	Sup.ic.Delete(ctx, f.inode)

	log.LogDebugf("TRACE Flush: ino(%v) (%v)", f.inode, time.Since(start))
	return nil
}

// Fsync hanldes the fsync request.
func (f *Node) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	// fsync from saveFuseContext doesn't have req
	if req != nil && req.Dir {
		return nil
	}
	tpObject := exporter.NewVolumeTPUs("Fsync_us", Sup.volname)
	tpObject1 := exporter.NewModuleTP("filefsync")
	defer func() {
		tpObject.Set(err)
		tpObject1.Set(err)
	}()

	log.LogDebugf("TRACE Fsync enter: ino(%v)", f.inode)
	start := time.Now()

	err = Sup.ec.Flush(ctx, f.inode)
	if err != nil {
		msg := fmt.Sprintf("Fsync: ino(%v) err(%v)", f.inode, err)
		Sup.handleErrorWithGetInode("Fsync", msg, f.inode)
		return fuse.EIO
	}

	log.LogDebugf("TRACE Fsync: ino(%v) (%v)", f.inode, time.Since(start))
	return nil
}

// Setattr handles the setattr request.
func (f *Node) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) (err error) {
	tpObject := exporter.NewVolumeTPUs("Setattr_us", Sup.volname)
	tpObject1 := exporter.NewModuleTP("filesetattr")
	defer func() {
		tpObject.Set(err)
		tpObject1.Set(err)
	}()

	ino := f.inode
	start := time.Now()
	info, err := Sup.InodeGet(ctx, ino)
	if err != nil {
		log.LogErrorf("Setattr: InodeGet failed, ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if req.Valid.Size() && (req.Size == 0 || !proto.IsDbBack) {
		if !f.havePermission(proto.XATTR_FLOCK_FLAG_WRITE) {
			return fuse.EPERM
		}
		if err := Sup.ec.Flush(ctx, ino); err != nil {
			log.LogErrorf("Setattr: truncate wait for flush ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		if err := Sup.ec.Truncate(ctx, ino, info.Size, req.Size); err != nil {
			log.LogErrorf("Setattr: truncate ino(%v) size(%v) err(%v)", ino, req.Size, err)
			return ParseError(err)
		}
		Sup.ic.Delete(ctx, ino)
		Sup.ec.RefreshExtentsCache(ctx, ino)
	}

	if valid := setattr(info, req); valid != 0 {
		err = Sup.mw.Setattr(ctx, ino, valid, info.Mode, info.Uid, info.Gid, int64(info.AccessTime),
			int64(info.ModifyTime))
		if err != nil {
			Sup.ic.Delete(ctx, ino)
			return ParseError(err)
		}
	}

	fillAttr(info, &resp.Attr)
	log.LogDebugf("TRACE Setattr: ino(%v) req(%v) (%v)", ino, req, time.Since(start))
	return nil
}

// Readlink handles the readlink request.
func (f *Node) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	ino := f.inode
	info, err := Sup.InodeGet(ctx, ino)
	if err != nil {
		log.LogErrorf("Readlink: ino(%v) err(%v)", ino, err)
		return "", ParseError(err)
	}
	log.LogDebugf("TRACE Readlink: ino(%v) target(%v)", ino, info.TargetStr())
	return info.TargetStr(), nil
}

// Getxattr has not been implemented yet.
func (f *Node) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	if !Sup.enableXattr {
		return fuse.ENOSYS
	}
	ino := f.inode
	name := req.Name
	size := req.Size
	pos := req.Position

	// ignore system.* and security.* xattr, ls will request these automatically
	if strings.HasPrefix(name, "system.") || strings.HasPrefix(name, "security.") {
		return nil
	}
	info, err := Sup.mw.XAttrGet_ll(ctx, ino, name)
	if err != nil {
		log.LogErrorf("GetXattr: ino(%v) name(%v) err(%v)", ino, name, err)
		return ParseError(err)
	}
	value := info.Get(name)
	if pos > 0 && pos < uint32(len(value)) {
		value = value[pos:]
	}
	if size > 0 && size < uint32(len(value)) {
		value = value[:size]
	}
	resp.Xattr = value
	log.LogDebugf("TRACE GetXattr: ino(%v) name(%v)", ino, name)
	return nil
}

// Listxattr has not been implemented yet.
func (f *Node) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	if !Sup.enableXattr {
		return fuse.ENOSYS
	}
	ino := f.inode
	_ = req.Size     // ignore currently
	_ = req.Position // ignore currently

	keys, err := Sup.mw.XAttrsList_ll(ctx, ino)
	if err != nil {
		log.LogErrorf("ListXattr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}
	for _, key := range keys {
		resp.Append(key)
	}
	log.LogDebugf("TRACE Listxattr: ino(%v)", ino)
	return nil
}

// Setxattr has not been implemented yet.
func (f *Node) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	if !Sup.enableXattr {
		return fuse.ENOSYS
	}
	ino := f.inode
	name := req.Name
	value := req.Xattr

	var flock *proto.XAttrFlock
	if name == proto.XATTR_FLOCK {
		var tmpFlock proto.XAttrFlock
		if err := json.Unmarshal(value, &tmpFlock); err != nil || tmpFlock.WaitTime == 0 {
			return fuse.EPERM
		}
		flock = &tmpFlock
	}
	// TODO： implement flag to improve compatible (Mofei Zhang)
	if err := Sup.mw.XAttrSet_ll(ctx, ino, []byte(name), []byte(value)); err != nil {
		log.LogErrorf("Setxattr: ino(%v) name(%v) err(%v)", ino, name, err)
		return ParseError(err)
	}
	if flock != nil {
		Sup.ic.Delete(ctx, ino)
		time.Sleep(time.Duration(flock.WaitTime)*time.Second + 100*time.Millisecond)
	}
	log.LogDebugf("TRACE Setxattr: ino(%v) name(%v)", ino, name)
	return nil
}

// Removexattr has not been implemented yet.
func (f *Node) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	if !Sup.enableXattr {
		return fuse.ENOSYS
	}
	ino := f.inode
	name := req.Name
	if err := Sup.mw.XAttrDel_ll(ctx, ino, name); err != nil {
		log.LogErrorf("Removexattr: ino(%v) name(%v) err(%v)", ino, name, err)
		return ParseError(err)
	}
	log.LogDebugf("TRACE RemoveXattr: ino(%v) name(%v)", ino, name)
	return nil
}

func (f *Node) fileSize(ctx context.Context, ino uint64) (size uint64, gen uint64) {
	size, gen, valid := Sup.ec.FileSize(ino)
	log.LogDebugf("fileSize: ino(%v) fileSize(%v) gen(%v) valid(%v)", ino, size, gen, valid)

	if !valid {
		if info, err := Sup.InodeGet(ctx, ino); err == nil {
			size = info.Size
			gen = info.Generation
		}
	}
	return
}

func (f *Node) havePermission(permBit int) bool {
	if !Sup.Flock() {
		return true
	}
	permBits, err := f.getPermBits()
	log.LogDebugf("ino(%v) permBit(%v) permBits(%v) err(%v)", f.inode, permBit, permBits, err)
	if err != nil {
		return false
	}
	if permBits == 0 {
		return true
	} else if permBits < 0 {
		return false
	} else {
		return permBits&permBit > 0
	}
}

// 0: permitted, -1: not permitted, otherwise: permission bits
func (f *Node) getPermBits() (bits int, err error) {
	var msg string
	defer func() {
		if err != nil {
			log.LogErrorf("ino(%v) err(%v)", f.inode, err)
		} else {
			log.LogDebugf("ino(%v) bits(%v) msg(%v) err(%v)", f.inode, bits, msg, err)
		}
	}()

	var info *proto.InodeInfo
	ino := f.inode
	for {
		if info != nil {
			if info.Inode == Sup.rootIno {
				return
			}
			if parentIno, ok := Sup.ic.GetParent(info.Inode); ok {
				ino = parentIno
			} else {
				err = fmt.Errorf("cannot get parent of %v", info.Inode)
				return
			}
		}
		if info, err = Sup.InodeGet(nil, ino); err != nil {
			return
		}
		flock := f.getValidFlock(info)
		if flock == nil {
			continue
		}

		ipPort := fmt.Sprintf("%s:%d", Sup.ec.LocalIp(), Sup.ProfPort)
		if flock.IpPort != ipPort {
			bits = -1
			msg = fmt.Sprintf("ipPort(%v) flock ipPort(%v)", ipPort, flock.IpPort)
		} else {
			bits = int(flock.Flag)
		}
		return
	}
}

func (f *Node) getValidFlock(info *proto.InodeInfo) (flock *proto.XAttrFlock) {
	xattrs := info.XAttrs()
	if xattrs == nil {
		return
	}
	for _, xattr := range *xattrs {
		if xattr.Name != proto.XATTR_FLOCK {
			continue
		}
		tmpFlock, ok := xattr.Value.(proto.XAttrFlock)
		if !ok || (tmpFlock.ValidTime > 0 && time.Now().Unix() > int64(tmpFlock.ValidTime)) {
			continue
		}
		if f.inode != info.Inode && tmpFlock.Flag&proto.XATTR_FLOCK_FLAG_RECURSIVE == 0 {
			continue
		}
		flock = &tmpFlock
	}
	return
}
