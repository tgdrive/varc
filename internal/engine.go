// Package internal provides the core caching engine for the varc HTTP proxy.
// This is a fork of rclone's VFS with all rclone fs.Object/fs.Fs
// dependencies removed, replaced with the RemoteObject interface.
package internal

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tgdrive/varc/internal/cache"
	"github.com/tgdrive/varc/internal/types"
)

// Node represents either a directory (*Dir) or a file (*File)
type Node interface {
	os.FileInfo
	IsFile() bool
	Inode() uint64
	SetModTime(modTime time.Time) error
	Sync() error
	Remove() error
	RemoveAll() error
	Engine() *Engine
	Open(flags int) (Handle, error)
	Truncate(size int64) error
	Path() string
	SetSys(any)
}

// Check interfaces
var (
	_ Node = (*File)(nil)
	_ Node = (*Dir)(nil)
)

// Nodes is a slice of Node
type Nodes []Node

func (ns Nodes) Len() int           { return len(ns) }
func (ns Nodes) Swap(i, j int)      { ns[i], ns[j] = ns[j], ns[i] }
func (ns Nodes) Less(i, j int) bool { return ns[i].Path() < ns[j].Path() }

// Noder represents something which can return a node
type Noder interface {
	fmt.Stringer
	Node() Node
}

// Check interfaces
var (
	_ Noder = (*ReadFileHandle)(nil)
)

// OsFiler is the methods on *os.File
type OsFiler interface {
	Chdir() error
	Chmod(mode os.FileMode) error
	Chown(uid, gid int) error
	Close() error
	Fd() uintptr
	Name() string
	Read(b []byte) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	Readdir(n int) ([]os.FileInfo, error)
	Readdirnames(n int) (names []string, err error)
	Seek(offset int64, whence int) (ret int64, err error)
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	Write(b []byte) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	WriteString(s string) (n int, err error)
}

// Handle is the interface satisfied by open files or directories.
type Handle interface {
	OsFiler
	Flush() error
	Release() error
	Node() Node
	Lock() error
	Unlock() error
}

// baseHandle implements all the missing methods
type baseHandle struct{}

func (h baseHandle) Chdir() error                                         { return ENOSYS }
func (h baseHandle) Chmod(mode os.FileMode) error                         { return ENOSYS }
func (h baseHandle) Chown(uid, gid int) error                             { return ENOSYS }
func (h baseHandle) Close() error                                         { return ENOSYS }
func (h baseHandle) Fd() uintptr                                          { return 0 }
func (h baseHandle) Name() string                                         { return "" }
func (h baseHandle) Read(b []byte) (n int, err error)                     { return 0, ENOSYS }
func (h baseHandle) ReadAt(b []byte, off int64) (n int, err error)        { return 0, ENOSYS }
func (h baseHandle) Readdir(n int) ([]os.FileInfo, error)                 { return nil, ENOSYS }
func (h baseHandle) Readdirnames(n int) (names []string, err error)       { return nil, ENOSYS }
func (h baseHandle) Seek(offset int64, whence int) (ret int64, err error) { return 0, ENOSYS }
func (h baseHandle) Stat() (os.FileInfo, error)                           { return nil, ENOSYS }
func (h baseHandle) Sync() error                                          { return nil }
func (h baseHandle) Truncate(size int64) error                            { return ENOSYS }
func (h baseHandle) Write(b []byte) (n int, err error)                    { return 0, ENOSYS }
func (h baseHandle) WriteAt(b []byte, off int64) (n int, err error)       { return 0, ENOSYS }
func (h baseHandle) WriteString(s string) (n int, err error)              { return 0, ENOSYS }
func (h baseHandle) Flush() (err error)                                   { return ENOSYS }
func (h baseHandle) Release() (err error)                                 { return ENOSYS }
func (h baseHandle) Node() Node                                           { return nil }
func (h baseHandle) Unlock() error                                        { return os.ErrInvalid }
func (h baseHandle) Lock() error                                          { return os.ErrInvalid }

// Check interfaces
var (
	_ OsFiler = (*os.File)(nil)
	_ Handle  = (*baseHandle)(nil)
	_ Handle  = (*ReadFileHandle)(nil)
	_ Handle  = (*DirHandle)(nil)
)

// Engine represents the top level caching engine
type Engine struct {
	ctx       context.Context
	root      *Dir
	Opt       types.Options
	cache     *cache.Cache
	cancel    context.CancelFunc
	usageMu   sync.Mutex
	usageTime time.Time
	pollChan  chan time.Duration
	inUse     atomic.Int32
}

// New creates a new Engine and root directory.
func New(ctx context.Context, opt *types.Options) (*Engine, error) {
	ctx, cancel := context.WithCancel(ctx)
	eng := &Engine{
		ctx:    ctx,
		cancel: cancel,
	}
	eng.inUse.Store(1)

	if opt != nil {
		eng.Opt = *opt
	}

	eng.Opt.Init()

	// Create cache
	ccache, err := cache.New(ctx, &eng.Opt, eng.addVirtual)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("engine: failed to create cache: %w", err)
	}
	eng.cache = ccache

	// Create root directory
	eng.root = newDir(eng, nil, "/")

	return eng, nil
}

// Keep track of active Engines keyed on the name
var (
	activeMu sync.Mutex
	active   = map[string][]*Engine{}
)

// addVirtual is called when the cache creates a virtual entry
func (e *Engine) addVirtual(remote string, size int64, isDir bool) error {
	// In our fork, we don't create virtual entries in the engine tree
	// This is a simplified no-op
	return nil
}

// Root returns the root directory
func (e *Engine) Root() *Dir {
	return e.root
}

// Usage returns the disk usage of the current Engine
func (e *Engine) Usage() (total, used int64) {
	e.usageMu.Lock()
	defer e.usageMu.Unlock()
	// Return a basic estimate based on cache metrics or return 0
	return 0, 0
}

// Context returns the Engine context
func (e *Engine) Context() context.Context {
	return e.ctx
}

// OpenFile opens a file for reading
func (e *Engine) OpenFile(name string) (Handle, error) {
	// Resolve the name relative to root
	name = strings.Trim(name, "/")
	return e.root.OpenFile(name, os.O_RDONLY, 0777)
}

// Close shuts down the Engine
func (e *Engine) Close() error {
	if e.cache != nil {
		_ = e.cache.CleanUp()
	}
	e.cancel()
	e.inUse.Store(0)
	return nil
}

// ReadFileInto reads a full file into the writer
func (e *Engine) ReadFileInto(ctx context.Context, name string, w io.Writer) error {
	handle, err := e.OpenFile(name)
	if err != nil {
		return err
	}
	defer handle.Close()
	_, err = io.Copy(w, handle)
	return err
}

var inodeCount atomic.Uint64

// newInode creates a new unique inode number
func newInode() (inode uint64) {
	return inodeCount.Add(1)
}

// Stat finds the Node by path starting from the root
func (e *Engine) Stat(path string) (node Node, err error) {
	return e.root.Stat(path)
}

// Open opens a file by path
func (e *Engine) Open(path string) (Handle, error) {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil, os.ErrInvalid
	}
	return e.root.OpenFile(path, os.O_RDONLY, 0)
}

// OpenCached opens a file for reading, optionally associating a RemoteObject
// for cache population. If obj is nil, only the local cache is used.
func (e *Engine) OpenCached(filePath string, obj types.RemoteObject) (Handle, error) {
	filePath = strings.Trim(filePath, "/")
	if filePath == "" {
		return nil, os.ErrInvalid
	}

	// Get or create the cache item
	item := e.cache.Item(filePath)
	if item == nil {
		return nil, fmt.Errorf("failed to create cache item for %s", filePath)
	}

	// Open the cache item with the remote object if provided
	if obj != nil {
		if err := item.Open(obj); err != nil {
			return nil, fmt.Errorf("failed to open cache item: %w", err)
		}
	}

	// Ensure the file node exists in the engine tree
	_, err := e.root.Stat(filePath)
	if err != nil {
		d := e.root
		f := newFile(e.ctx, d, filePath)
		if size, err := item.GetSize(); err == nil {
			f.size.Store(size)
		}
		d.AddChild(filePath, f)
	}

	return e.root.OpenFile(filePath, os.O_RDONLY, 0)
}

// CacheItem returns the cache item for a path, creating it if needed
func (e *Engine) CacheItem(path string) *cache.Item {
	return e.cache.Item(path)
}

// Remove removes an item from the cache by name.
// Returns nil on success. If the cache is nil, returns nil.
func (e *Engine) Remove(name string) error {
	if e.cache == nil {
		return nil
	}
	e.cache.Remove(name)
	return nil
}

// Stats returns cache statistics from the underlying cache engine.
func (e *Engine) Stats() map[string]interface{} {
	if e.cache == nil {
		return map[string]interface{}{}
	}
	return e.cache.Stats()
}
