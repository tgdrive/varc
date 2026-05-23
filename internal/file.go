package internal

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tgdrive/varc/internal/types"
)

// File represents a file or a symlink
type File struct {
	inode uint64          // inode number - read only
	size  atomic.Int64    // size of file
	ctx   context.Context // context for engine operations - read only

	muRW sync.Mutex // reserved for operations that must not race with File.Remove

	mu   sync.RWMutex // protects the following
	d    *Dir         // parent directory
	name string       // name of the file relative to the root

	// Cache
	remote types.RemoteObject // remote object backing this file

	sys atomic.Value // system level info
}

// newFile creates a new File object
func newFile(ctx context.Context, d *Dir, name string) *File {
	return &File{
		inode: newInode(),
		ctx:   ctx,
		d:     d,
		name:  name,
	}
}

// String returns the name of the file
func (f *File) String() string {
	return f.name
}

// Path returns the path of the file relative to engine root
func (f *File) Path() string {
	return f.name
}

// IsFile is always true for File
func (f *File) IsFile() bool {
	return true
}

// IsDir is always false for File
func (f *File) IsDir() bool {
	return false
}

// ModTime returns the modified time of the file
func (f *File) ModTime() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.modTime()
}

func (f *File) modTime() time.Time {
	if f.d == nil || f.d.engine == nil {
		return time.Time{}
	}
	item := f.d.engine.CacheItem(f.Path())
	if item == nil {
		return time.Time{}
	}
	modTime, err := item.GetModTime()
	if err != nil {
		return time.Time{}
	}
	return modTime
}

// Size returns the size of the file
func (f *File) Size() int64 {
	return f.size.Load()
}

// Name returns the base name of the file
func (f *File) Name() string {
	return f.name
}

// Sys returns the underlying data source (can be nil)
func (f *File) Sys() any {
	return f.sys.Load()
}

// SetSys sets the underlying data source
func (f *File) SetSys(x any) {
	f.sys.Store(x)
}

// Inode returns the inode number
func (f *File) Inode() uint64 {
	return f.inode
}

// Engine returns the parent Engine
func (f *File) Engine() *Engine {
	return f.d.engine
}

// DirEntry returns the DirEntry for this file
func (f *File) DirEntry() os.FileInfo {
	return f
}

// Open opens the file with the given flags.
// If the file is not in cache, it uses the cached remote object to open it.
func (f *File) Open(flags int) (fh Handle, err error) {
	switch flags & accessModeMask {
	case os.O_RDONLY:
		return newReadFileHandle(f)
	// For simplicity, write modes return error
	default:
		return nil, EPERM
	}
}

// SetModTime sets the modification time of the file
func (f *File) SetModTime(modTime time.Time) error {
	return nil
}

// Sync syncs the file - no-op for our read-only mode
func (f *File) Sync() error {
	return nil
}

// Remove removes the file
func (f *File) Remove() error {
	return nil
}

// RemoveAll removes the file
func (f *File) RemoveAll() error {
	return nil
}

// Truncate truncates the file
func (f *File) Truncate(size int64) error {
	return ENOSYS
}

// Mode returns the file mode
func (f *File) Mode() os.FileMode {
	return 0644
}

// accessModeMask masks off extra bits from os.OpenFile flags
const accessModeMask = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
