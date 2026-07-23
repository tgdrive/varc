package varc

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// Prune performs age, size, invalid-entry, and free-space cleanup. It is safe
// to call while readers are active; invalid metadata is removed only when its
// data file is also absent.
func (c *Cache) Prune(ctx context.Context) (PruneStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return PruneStats{}, ErrClosed
	}
	var stats PruneStats
	entries, err := c.ListEntries(ctx)
	if err != nil {
		stats.Errors = append(stats.Errors, err)
	}
	stats.Scanned = len(entries)
	stats.BytesBefore = sumEntryBytes(entries)
	stats.FreeBefore = diskFree(c.dir)
	now := time.Now()
	var survivors []EntryInfo
	for _, e := range entries {
		if ctx.Err() != nil {
			stats.Errors = append(stats.Errors, ctx.Err())
			break
		}
		if e.MetadataErr != nil {
			if e.OpenReaders > 0 || e.ActiveFetches > 0 || e.OnDisk {
				survivors = append(survivors, e)
				continue
			}
			stats.ReasonInvalid++
			c.removeEntryInfo(e, &stats)
			continue
		}
		if e.OpenReaders > 0 || e.ActiveFetches > 0 || e.Pinned {
			survivors = append(survivors, e)
			continue
		}
		if c.cacheMaxAge > 0 && !e.AccessedAt.IsZero() && now.Sub(e.AccessedAt) > c.cacheMaxAge {
			stats.ReasonAge++
			c.removeEntryInfo(e, &stats)
			continue
		}
		survivors = append(survivors, e)
	}
	bytes := sumEntryBytes(survivors)
	if c.cacheMaxSize > 0 && bytes > c.cacheMaxSize {
		sort.Slice(survivors, func(i, j int) bool { return survivors[i].AccessedAt.Before(survivors[j].AccessedAt) })
		kept := survivors[:0]
		for _, e := range survivors {
			if bytes <= c.cacheMaxSize {
				kept = append(kept, e)
				continue
			}
			if e.OpenReaders > 0 || e.ActiveFetches > 0 || e.Pinned {
				kept = append(kept, e)
				continue
			}
			stats.ReasonSize++
			c.removeEntryInfo(e, &stats)
			bytes -= e.DataBytes
		}
		survivors = kept
	}
	if c.cacheMinFreeSpace > 0 {
		free := diskFree(c.dir)
		if free >= 0 && free < c.cacheMinFreeSpace {
			sort.Slice(survivors, func(i, j int) bool { return survivors[i].AccessedAt.Before(survivors[j].AccessedAt) })
			for _, e := range survivors {
				if free >= c.cacheMinFreeSpace {
					break
				}
				if e.OpenReaders > 0 || e.ActiveFetches > 0 || e.Pinned {
					continue
				}
				stats.ReasonFree++
				c.removeEntryInfo(e, &stats)
				free = diskFree(c.dir)
			}
		}
	}
	_, after := c.scanDataUsage()
	stats.BytesAfter = after
	stats.FreeAfter = diskFree(c.dir)
	return stats, joinErrors(stats.Errors...)
}

func (c *Cache) removeEntryInfo(e EntryInfo, stats *PruneStats) {
	if e.Path == "" && e.MetaPath != "" {
		e.Path = strings.TrimSuffix(e.MetaPath, ".meta")
	}
	bytes := e.DataBytes
	if bytes == 0 {
		bytes = dataFileSize(e.Path)
	}
	if err := c.removePath(e.Path); err != nil {
		stats.Errors = append(stats.Errors, err)
		return
	}
	stats.Removed++
	stats.RemovedBytes += bytes
}

func (c *Cache) janitor() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(c.ctx, c.pollInterval)
			_, err := c.Prune(ctx)
			cancel()
			c.metricBackgroundPrunes.Add(1)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
				c.logger.Warnf("varc: background prune failed: %v", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// Verify checks metadata/data consistency and optionally block checksums.
func (c *Cache) Verify(ctx context.Context) (VerifyStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entries, err := c.ListEntries(ctx)
	stats := VerifyStats{Entries: len(entries)}
	if err != nil {
		stats.Errors = append(stats.Errors, err)
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			stats.Errors = append(stats.Errors, ctx.Err())
			break
		}
		if e.MetadataErr != nil {
			stats.CorruptMeta++
			stats.Errors = append(stats.Errors, e.MetadataErr)
			continue
		}
		if !e.OnDisk {
			stats.MissingData++
			continue
		}
		if !rangesValid(e.Ranges, e.Size) {
			stats.BadRanges++
			continue
		}
		if e.Complete {
			stats.Complete++
		} else {
			stats.Incomplete++
		}
		if c.verifyChecksum {
			meta, ok, err := loadMeta(e.MetaPath)
			if err != nil || !ok {
				stats.ChecksumErrors++
				if err != nil {
					stats.Errors = append(stats.Errors, err)
				}
				continue
			}
			if err := verifyChecksums(e.Path, meta.Checksums); err != nil {
				stats.ChecksumErrors++
				stats.Errors = append(stats.Errors, err)
			}
		}
	}
	return stats, joinErrors(stats.Errors...)
}

func verifyChecksums(path string, sums []blockChecksum) error {
	if len(sums) == 0 {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, mebi)
	for _, s := range sums {
		if s.End < s.Start {
			return fmt.Errorf("varc: bad checksum range %d-%d", s.Start, s.End)
		}
		need := s.End - s.Start
		if int64(len(buf)) < need {
			buf = make([]byte, need)
		}
		n, err := readFullAt(f, buf[:need], s.Start)
		if err != nil {
			return err
		}
		if int64(n) != need {
			return io.ErrUnexpectedEOF
		}
		got := crc32.ChecksumIEEE(buf[:need])
		if got != s.CRC32 {
			return fmt.Errorf("varc: checksum mismatch %s %d-%d", path, s.Start, s.End)
		}
	}
	return nil
}

// WarmRange schedules and waits for a range to be cached.  It is useful for
// mounting layers that want to pre-buffer file headers, media indexes, or small
// sidecar objects before serving a request.
