// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuse

import (
	"errors"
	"github.com/takeshi-yoshimura/fuse/fuseops"
	"github.com/takeshi-yoshimura/fuse/internal/buffer"
	"github.com/takeshi-yoshimura/fuse/internal/fusekernel"
	"syscall"
)

// Notifier gives APIs to invalidate cache from user to kernel without exposing Connection
type Notifier struct {
	conn *Connection
}

// InvalidateEntry notifies to invalidate parent attributes and the dentry matching parent/name.
//
// To avoid a deadlock this function must not be called in the execution path of a related filesytem
// operation or within any code that could hold a lock that could be needed to execute such an operation.
// As of kernel 4.18, a "related operation" is a lookup(), symlink(), mknod(), mkdir(), unlink(),
// rename(), link() or create() request for the parent, and a setattr(), unlink(), rmdir(), rename(),
// setxattr(), removexattr(), readdir() or readdirplus() request for the inode itself.
//
// When called correctly, this function will never block.
//
// Added in FUSE protocol version 7.12. If the kernel does not support this (or a newer) version,
// the function will return -ENOSYS and do nothing.
//
// Parameters: parent: inode number, name: file name
// Return: zero for success, -errno for failure
func (n *Notifier) InvalidateEntry(parent fuseops.InodeID, name string) error {
	return n.notifyKernel(&fuseops.NotifyInvalEntryOp{Parent: parent, Name: name})
}

// NotifyDelete behaves like InvalidateEntry with the following additional effect (at least
// as of Linux kernel 4.8):
//
// If the provided child inode matches the inode that is currently associated with the cached
// dentry, and if there are any inotify watches registered for the dentry, then the watchers are
// informed that the dentry has been deleted.
//
// To avoid a deadlock this function must not be called while executing a related filesytem
// operation or while holding a lock that could be needed to execute such an operation (see the
// description of InvalidateEntry for more details).
//
// When called correctly, this function will never block.
//
// Added in FUSE protocol version 7.18. If the kernel does not support this (or a newer) version,
// the function will return -ENOSYS and do nothing.
//
// Parameters: parent: inode number, child: inode number, name: file name
// Returns: zero for success, -errno for failure
func (n *Notifier) NotifyDelete(parent fuseops.InodeID, child fuseops.InodeID, name string) error {
	return n.notifyKernel(&fuseops.NotifyDeleteOp{Parent: parent, Child: child, Name: name})
}

// InvalidateInode notifies to invalidate cache for an inode.
// Added in FUSE protocol version 7.12. If the kernel does not support this (or a newer) version,
// the function will return -ENOSYS and do nothing.
//
// If the filesystem has writeback caching enabled, invalidating an inode will first trigger a
// writeback of all dirty pages. The call will block until all writeback requests have completed
// and the inode has been invalidated. It will, however, not wait for completion of pending writeback
// requests that have been issued before.
//
// If there are no dirty pages, this function will never block.
//
// Parameters:
// ino: the inode number
// off: the offset in the inode where to start invalidating or negative to invalidate attributes only
// len: the amount of cache to invalidate or 0 for all
// Return: zero for success, -errno for failure
func (n *Notifier) InvalidateInode(ino fuseops.InodeID, off int64, len int64) error {
	return n.notifyKernel(&fuseops.NotifyInvalInodeOp{Ino: ino, Off: off, Len: len})
}

// Store stores data to the kernel buffers
// Synchronously store data in the kernel buffers belonging to the given inode. The stored data is marked
// up-to-date (no read will be performed against it, unless it's invalidated or evicted from the cache).
//
// If the stored data overflows the current file size, then the size is extended, similarly to a write(2)
// on the filesystem.
//
// If this function returns an error, then the store wasn't fully completed, but it may have been partially
// completed.
//
// Added in FUSE protocol version 7.15. If the kernel does not support this (or a newer) version, the
// function will return -ENOSYS and do nothing.
//
// Parameters:
// ino: the inode number
// offset: the starting offset into the file to store to
// bufv: buffer vector (NOTE: only support a single buffer at this moment)
// Return: zero for success, -errno for failure
func (n *Notifier) Store(ino fuseops.InodeID, offset int64, bufv [][]byte) error {
	return n.notifyKernel(&fuseops.NotifyStoreOp{Ino: ino, Offset: offset, Bufv: bufv})
}

func (n *Notifier) buildNotify(m *buffer.OutMessage, op interface{}) error {
	h := m.OutHeader()
	h.Unique = 0
	// Create the appropriate output message
	switch o := op.(type) {
	case *fuseops.NotifyInvalInodeOp:
		h.Error = fusekernel.NotifyCodeInvalInode
		size := fusekernel.NotifyInvalInodeOutSize
		out := (*fusekernel.NotifyInvalInodeOut)(m.Grow(size))
		out.Ino = uint64(o.Ino)
		out.Off = o.Off
		out.Len = o.Len

	case *fuseops.NotifyInvalEntryOp:
		err := checkName(o.Name)
		if err != nil {
			return err
		}
		h.Error = fusekernel.NotifyCodeInvalEntry
		size := fusekernel.NotifyInvalEntryOutSize
		out := (*fusekernel.NotifyInvalEntryOut)(m.Grow(size))
		out.Parent = uint64(o.Parent)
		name := []byte(o.Name)
		name = append(name, '\x00')
		out.Namelen = uint32(len(name) - 1)
		m.Append(name)

	case *fuseops.NotifyDeleteOp:
		err := checkName(o.Name)
		if err != nil {
			return err
		}
		h.Error = fusekernel.NotifyCodeDelete
		size := fusekernel.NotifyDeleteOutSize
		out := (*fusekernel.NotifyDeleteOut)(m.Grow(size))
		out.Parent = uint64(o.Parent)
		out.Child = uint64(o.Child)
		name := []byte(o.Name)
		name = append(name, '\x00')
		out.Namelen = uint32(len(name) - 1)
		m.Append(name)

	case *fuseops.NotifyStoreOp:
		h.Error = fusekernel.NotifyCodeStore
		size := fusekernel.NotifyStoreOutSize
		out := (*fusekernel.NotifyStoreOut)(m.Grow(size))
		out.Nodeid = uint64(o.Ino)
		out.Offset = uint64(o.Offset)
		out.Size = 0
		for _, buf := range o.Bufv {
			out.Size += uint32(len(buf))
		}
		m.Append(o.Bufv...)

	default:
		return errors.New("unexpectedop")
	}
	h.Len = uint32(m.Len())

	return nil
}

// notifyKernel requests the kernel to update the state of an inode (e.g., invalidate cache) from the FUSE process
func (n *Notifier) notifyKernel(op interface{}) error {
	c := n.conn
	outMsg := c.getOutMessage()
	err := n.buildNotify(outMsg, op)
	if err != nil {
		return err
	}
	defer c.putOutMessage(outMsg)

	if c.debugLogger != nil {
		c.debugLog(outMsg.OutHeader().Unique, 1, "<- %s", describeRequest(op))
	}

	if outMsg.Sglist != nil {
		_, err = writev(int(c.dev.Fd()), outMsg.Sglist)
	} else {
		err = c.writeMessage(outMsg.OutHeaderBytes())
	}
	// Debug logging
	if c.debugLogger != nil {
		fuseID := outMsg.OutHeader().Unique
		if err == nil {
			c.debugLog(fuseID, 1, "-> OK (%s)", describeResponse(op))
		} else {
			c.debugLog(fuseID, 1, "-> Error: %q", err.Error())
		}
	}
	return nil
}

func checkName(name string) error {
	const maxUint32 = ^uint32(0)
	if uint64(len(name)) > uint64(maxUint32) {
		// very unlikely, but we don't want to silently truncate
		return syscall.ENAMETOOLONG
	}
	return nil
}
