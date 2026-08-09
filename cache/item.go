package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tgdrive/varc/internal/engine/ioerrors"
	"github.com/tgdrive/varc/internal/engine/scheduler"
	"github.com/tgdrive/varc/internal/engine/sparsefile"
	"github.com/tgdrive/varc/ranges"
	"github.com/tgdrive/varc/source"
)

type itemInfo struct {
	Key         string
	ATime       time.Time
	Size        int64
	Rs          ranges.Ranges
	Fingerprint string
}

// Item is one sparse cache file. Ranges describe which bytes in the
// apparent-size file are actually valid cached data.
type Item struct {
	c        *Cache
	key      string
	dataPath string
	metaPath string

	mu              sync.Mutex
	cond            *sync.Cond
	fd              *os.File
	info            itemInfo
	meta            source.Metadata
	object          *sourceObject
	downloaders     *scheduler.Downloaders
	loaded          bool
	opens           int
	graceTimer      *time.Timer
	closing         chan struct{}
	pendingAccesses int
	beingReset      bool
}

func newItem(c *Cache, key string) *Item {
	dataPath, metaPath := c.pathsForKey(key)
	item := &Item{c: c, key: key, dataPath: dataPath, metaPath: metaPath}
	item.cond = sync.NewCond(&item.mu)
	return item
}

func (item *Item) createDownloadersLocked() {
	if item.downloaders != nil || item.object == nil || item.info.Size <= 0 {
		return
	}
	opt := &scheduler.Config{
		ChunkSize:      item.c.opt.ChunkSize,
		ChunkSizeLimit: item.c.opt.ChunkSizeLimit,
		ChunkStreams:   item.c.opt.ChunkStreams,
		ReadAhead:      item.c.opt.ReadAhead,
	}
	item.downloaders = scheduler.New(item.c.ctx, item, opt, item.key, item.object)
}

func (item *Item) openLocked(ctx context.Context, sourceKey string, meta source.Metadata) error {
	for item.closing != nil {
		closing := item.closing
		item.mu.Unlock()
		<-closing
		item.mu.Lock()
	}
	if item.graceTimer != nil {
		item.graceTimer.Stop()
		item.graceTimer = nil
	}
	if !item.loaded {
		if err := item.loadLocked(); err != nil {
			return fmt.Errorf("cache: load %q: %w", item.key, err)
		}
		item.loaded = true
	}

	fingerprint := metadataFingerprint(meta)
	if item.opens > 0 {
		if item.info.Fingerprint != fingerprint {
			return fmt.Errorf("cache: %q: %w", item.key, source.ErrObjectChanged)
		}
	} else if item.info.Rs.Size() != 0 && (fingerprint == "" || item.info.Fingerprint == "" || item.info.Fingerprint != fingerprint) {
		if err := item.resetLocked(); err != nil {
			return fmt.Errorf("cache: reset changed object %q: %w", item.key, err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(item.dataPath), 0o700); err != nil {
		return fmt.Errorf("cache: create data shard: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(item.metaPath), 0o700); err != nil {
		return fmt.Errorf("cache: create metadata shard: %w", err)
	}

	if item.fd == nil {
		_, statErr := os.Stat(item.dataPath)
		if errors.Is(statErr, os.ErrNotExist) && item.info.Rs.Size() != 0 {
			// Metadata without its sparse data file is never trusted.
			item.info.Rs = nil
		}
		fd, err := os.OpenFile(item.dataPath, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("cache: open data file: %w", err)
		}
		item.fd = fd
		_ = sparsefile.SetSparse(item.fd)
	}
	if stat, err := item.fd.Stat(); err != nil {
		return fmt.Errorf("cache: stat data file: %w", err)
	} else if stat.Size() != meta.Size {
		if err := item.fd.Truncate(meta.Size); err != nil {
			return fmt.Errorf("cache: truncate data file to %d: %w", meta.Size, err)
		}
		item.info.Rs = item.info.Rs.Intersection(ranges.Range{Pos: 0, Size: meta.Size})
	}

	if item.object == nil {
		item.object = &sourceObject{src: item.c.src}
	}
	item.object.updateRequest(sourceKey, meta, source.RequestHeaders(ctx))
	item.meta = meta
	item.info.Key = item.key
	item.info.Size = meta.Size
	item.info.Fingerprint = fingerprint
	item.info.ATime = time.Now()
	item.createDownloadersLocked()
	if err := item.saveLocked(); err != nil {
		return err
	}
	item.opens++
	return nil
}

func (item *Item) loadLocked() error {
	in, err := os.Open(item.metaPath)
	if errors.Is(err, os.ErrNotExist) {
		item.info = itemInfo{Key: item.key, ATime: time.Now()}
		return nil
	}
	if err != nil {
		return err
	}
	defer in.Close()

	var info itemInfo
	if err := json.NewDecoder(in).Decode(&info); err != nil {
		// A corrupt metadata file must never make sparse bytes trusted.
		_ = os.Remove(item.metaPath)
		_ = os.Remove(item.dataPath)
		item.info = itemInfo{Key: item.key, ATime: time.Now()}
		return nil
	}
	if info.Key != item.key {
		return fmt.Errorf("metadata key %q does not match %q", info.Key, item.key)
	}
	item.info = info
	return nil
}

func (item *Item) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(item.metaPath), 0o700); err != nil {
		return fmt.Errorf("cache: create metadata directory: %w", err)
	}
	out, err := os.CreateTemp(filepath.Dir(item.metaPath), ".meta-*")
	if err != nil {
		return fmt.Errorf("cache: create metadata temp file: %w", err)
	}
	tmp := out.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()

	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "\t")
	if err := encoder.Encode(item.info); err != nil {
		_ = out.Close()
		return fmt.Errorf("cache: encode metadata: %w", err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return fmt.Errorf("cache: sync metadata: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("cache: close metadata: %w", err)
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return fmt.Errorf("cache: chmod metadata: %w", err)
	}
	if err := os.Rename(tmp, item.metaPath); err != nil {
		return fmt.Errorf("cache: replace metadata: %w", err)
	}
	ok = true
	return nil
}

func (item *Item) resetLocked() error {
	if item.downloaders != nil {
		dls := item.downloaders
		item.downloaders = nil
		item.mu.Unlock()
		err := dls.Close(nil)
		item.mu.Lock()
		if err != nil {
			return err
		}
	}
	if err := item.closeFDLocked(); err != nil {
		return err
	}
	if err := os.Remove(item.dataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(item.metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	item.info = itemInfo{Key: item.key, ATime: time.Now()}
	item.object = nil
	return nil
}

// resetForSpace empties clean cached ranges while preserving an open reader.
// It resets cached ranges in place when the quota cleaner needs space.
func (item *Item) resetForSpace() (removed bool, spaceFreed int64, err error) {
	item.mu.Lock()
	defer item.mu.Unlock()

	if item.opens == 0 && item.graceTimer == nil {
		spaceFreed = item.info.Rs.Size()
		if item.downloaders != nil {
			dls := item.downloaders
			item.downloaders = nil
			item.mu.Unlock()
			err = dls.Close(nil)
			item.mu.Lock()
			if err != nil {
				return false, 0, err
			}
		}
		_ = item.closeFDLocked()
		_ = os.Remove(item.dataPath)
		_ = os.Remove(item.metaPath)
		item.info = itemInfo{}
		item.loaded = false
		item.object = nil
		return true, spaceFreed, nil
	}
	if item.graceTimer != nil || item.pendingAccesses > 0 {
		return false, 0, nil
	}
	if item.info.Rs.Size() == 0 && !item.beingReset {
		return false, 0, nil
	}

	item.beingReset = true
	finishReset := func(clear bool) {
		if clear {
			item.beingReset = false
			item.cond.Broadcast()
		}
	}

	checkErr := func(e error) {
		if e != nil && err == nil {
			err = e
		}
	}
	if item.downloaders != nil {
		dls := item.downloaders
		item.downloaders = nil
		item.mu.Unlock()
		checkErr(dls.Close(nil))
		item.mu.Lock()
	}
	if item.fd != nil {
		checkErr(item.fd.Close())
		item.fd = nil
		if err != nil {
			finishReset(true)
			return false, 0, err
		}
	}

	spaceFreed = item.info.Rs.Size()
	_ = os.Remove(item.dataPath)
	_ = os.Remove(item.metaPath)
	item.info.Rs = nil

	if err = os.MkdirAll(filepath.Dir(item.dataPath), 0o700); err != nil {
		finishReset(!ioerrors.IsNoSpace(err))
		return false, spaceFreed, err
	}
	item.fd, err = os.OpenFile(item.dataPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		finishReset(!ioerrors.IsNoSpace(err))
		return false, spaceFreed, err
	}
	_ = sparsefile.SetSparse(item.fd)
	if err = item.fd.Truncate(item.info.Size); err != nil {
		_ = item.fd.Close()
		item.fd = nil
		finishReset(!ioerrors.IsNoSpace(err))
		return false, spaceFreed, err
	}
	if err = item.saveLocked(); err != nil {
		finishReset(!ioerrors.IsNoSpace(err))
		return false, spaceFreed, err
	}
	item.createDownloadersLocked()
	finishReset(true)
	return false, spaceFreed, nil
}

func (item *Item) closeFDLocked() error {
	if item.fd == nil {
		return nil
	}
	err := item.fd.Close()
	item.fd = nil
	return err
}

func (item *Item) release() error {
	item.preAccess()
	defer item.postAccess()
	item.mu.Lock()
	defer item.mu.Unlock()

	item.info.ATime = time.Now()
	item.opens--
	if item.opens < 0 {
		return os.ErrClosed
	}
	if item.opens > 0 {
		return nil
	}
	if item.c.opt.HandleCaching > 0 {
		item.graceTimer = time.AfterFunc(item.c.opt.HandleCaching, item.closeAfterGrace)
		return nil
	}
	return item.actualCloseLocked()
}

func (item *Item) closeAfterGrace() {
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.opens > 0 || item.graceTimer == nil {
		return
	}
	item.graceTimer = nil
	item.closing = make(chan struct{})
	_ = item.actualCloseLocked()
	close(item.closing)
	item.closing = nil
}

// actualCloseLocked stops range fetches before closing the sparse file,
// then close the sparse file, then persist the final range metadata.
func (item *Item) actualCloseLocked() (err error) {
	if item.downloaders != nil {
		dls := item.downloaders
		item.downloaders = nil
		item.mu.Unlock()
		dlsErr := dls.Close(nil)
		item.mu.Lock()
		if dlsErr != nil {
			err = dlsErr
		}
	}
	if closeErr := item.closeFDLocked(); closeErr != nil && err == nil {
		err = closeErr
	}
	if saveErr := item.saveLocked(); saveErr != nil && err == nil {
		err = saveErr
	}
	return err
}

func (item *Item) preAccess() {
	item.mu.Lock()
	defer item.mu.Unlock()
	for item.beingReset {
		item.cond.Wait()
	}
	item.pendingAccesses++
}

func (item *Item) postAccess() {
	item.mu.Lock()
	defer item.mu.Unlock()
	item.pendingAccesses--
	item.cond.Broadcast()
}

func (item *Item) readAt(ctx context.Context, b []byte, off int64) (n int, err error) {
	for retries := range item.c.opt.LowLevelRetries {
		item.preAccess()
		n, err = item.readAtOnce(ctx, b, off)
		item.postAccess()
		if err == nil || errors.Is(err, io.EOF) {
			break
		}
		if !ioerrors.IsNoSpace(err) && err.Error() != "no space left on device" {
			break
		}
		item.c.KickCleaner()
		time.Sleep(time.Duration(2<<uint(retries)) * time.Millisecond)
	}
	return n, err
}

func (item *Item) readAtOnce(_ context.Context, b []byte, off int64) (n int, err error) {
	item.mu.Lock()
	if item.fd == nil {
		item.mu.Unlock()
		return 0, errors.New("cache: item file is not open")
	}
	if off < 0 {
		item.mu.Unlock()
		return 0, io.EOF
	}
	defer item.mu.Unlock()

	r := ranges.Range{Pos: off, Size: int64(len(b))}
	if err = item.ensureLocked(r); err != nil {
		return 0, err
	}
	item.info.ATime = time.Now()
	return item.fd.ReadAt(b, off)
}

// ensureLocked ensures r is present. The item mutex must be held on entry and
// is temporarily released while coordinating the scheduler.
func (item *Item) ensureLocked(r ranges.Range) error {
	if r.End() > item.info.Size {
		r.Size = item.info.Size - r.Pos
	}
	present := item.info.Rs.Present(r)
	if !present && item.downloaders == nil {
		item.createDownloadersLocked()
	}
	dls := item.downloaders
	item.mu.Unlock()
	defer item.mu.Lock()

	if present {
		if dls == nil {
			return nil
		}
		return dls.EnsureDownloader(r)
	}
	if dls == nil {
		return errors.New("cache: scheduler is not available")
	}
	return dls.Download(r)
}

// HasRange returns true when the entire range is present in the sparse cache.
func (item *Item) HasRange(r ranges.Range) bool {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.info.Rs.Present(r)
}

// FindMissing returns the first missing part of r, clipped to object size.
func (item *Item) FindMissing(r ranges.Range) (out ranges.Range) {
	item.mu.Lock()
	defer item.mu.Unlock()
	out = item.info.Rs.FindMissing(r)
	out.Clip(item.info.Size)
	return out
}

// WriteAtNoOverwrite writes only byte ranges not already present in the cache.
func (item *Item) WriteAtNoOverwrite(b []byte, off int64) (n int, skipped int, err error) {
	item.mu.Lock()
	defer item.mu.Unlock()

	r := ranges.Range{Pos: off, Size: int64(len(b))}
	foundRanges := item.info.Rs.FindAll(r)
	for i := range foundRanges {
		foundRange := &foundRanges[i]
		if foundRange.R.Pos != off {
			return n, skipped, errors.New("cache: internal range offset mismatch")
		}
		size := int(foundRange.R.Size)
		var nn int
		if foundRange.Present {
			nn = size
			skipped += size
		} else {
			nn, err = item.fd.WriteAt(b[:size], off)
			if err == nil && nn != size {
				err = fmt.Errorf("cache: short write: tried %d bytes, wrote %d", size, nn)
			}
			item.info.Rs.Insert(ranges.Range{Pos: off, Size: int64(nn)})
		}
		off += int64(nn)
		b = b[nn:]
		n += nn
		if err != nil {
			break
		}
	}
	return n, skipped, err
}

func metadataFingerprint(meta source.Metadata) string {
	etag := strings.TrimSpace(meta.ETag)
	if strings.HasPrefix(etag, "W/") {
		etag = ""
	}
	if etag == "" && meta.LastModified.IsZero() {
		return ""
	}
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%d\n%s\n%d\n", meta.Size, etag, meta.LastModified.UTC().UnixNano())
	return hex.EncodeToString(hash.Sum(nil))
}

// Reader is an opened reference to a cached object.
type Reader struct {
	ctx  context.Context
	item *Item
	meta source.Metadata
}

// ReadAt implements io.ReaderAt using the context supplied to Cache.Open.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	return r.item.readAt(r.ctx, p, off)
}

// ReadAtContext performs a read using ctx instead of the open context.
func (r *Reader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	return r.item.readAt(ctx, p, off)
}

// Size returns the source size observed when this reader was opened.
func (r *Reader) Size() int64 { return r.meta.Size }

// Metadata returns the source metadata observed when this reader was opened.
func (r *Reader) Metadata() source.Metadata { return r.meta }

// Close releases this reader. The underlying file handle may remain alive for
// Options.HandleCaching.
func (r *Reader) Close() error {
	return r.item.release()
}

var _ io.ReaderAt = (*Reader)(nil)
var _ io.Closer = (*Reader)(nil)
