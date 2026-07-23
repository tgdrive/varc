package varc

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

func normalizeRanges(ranges []byteRange, size int64) []byteRange {
	if len(ranges) == 0 || size < 0 {
		return nil
	}
	clean := make([]byteRange, 0, len(ranges))
	for _, r := range ranges {
		if r.End <= r.Start || r.End <= 0 || r.Start >= size {
			continue
		}
		if r.Start < 0 {
			r.Start = 0
		}
		if r.End > size {
			r.End = size
		}
		clean = append(clean, r)
	}
	if len(clean) == 0 {
		return nil
	}
	sort.Slice(clean, func(i, j int) bool {
		if clean[i].Start == clean[j].Start {
			return clean[i].End < clean[j].End
		}
		return clean[i].Start < clean[j].Start
	})
	merged := clean[:0]
	for _, r := range clean {
		if len(merged) == 0 || r.Start > merged[len(merged)-1].End {
			merged = append(merged, r)
			continue
		}
		if r.End > merged[len(merged)-1].End {
			merged[len(merged)-1].End = r.End
		}
	}
	return append([]byteRange(nil), merged...)
}

func addRange(ranges []byteRange, start, end int64) []byteRange {
	if end <= start {
		return normalizeRanges(ranges, math.MaxInt64)
	}
	ranges = append(ranges, byteRange{Start: start, End: end})
	return normalizeRanges(ranges, math.MaxInt64)
}

func containsRange(ranges []byteRange, start, end int64) bool {
	if end <= start {
		return true
	}
	for _, r := range ranges {
		if r.Start <= start && r.End >= end {
			return true
		}
		if r.Start > start {
			return false
		}
	}
	return false
}

func firstMissingRange(ranges []byteRange, start, end int64) (int64, int64, bool) {
	if end <= start {
		return 0, 0, false
	}
	pos := start
	sorted := normalizeRanges(ranges, math.MaxInt64)
	for _, r := range sorted {
		if r.End <= pos {
			continue
		}
		if r.Start > pos {
			return pos, min64(r.Start, end), true
		}
		if r.End > pos {
			pos = r.End
		}
		if pos >= end {
			return 0, 0, false
		}
	}
	if pos < end {
		return pos, end, true
	}
	return 0, 0, false
}

func missingRanges(ranges []byteRange, start, end int64) []byteRange {
	var out []byteRange
	pos := start
	sorted := normalizeRanges(ranges, math.MaxInt64)
	for _, r := range sorted {
		if r.End <= pos {
			continue
		}
		if r.Start > pos {
			out = append(out, byteRange{Start: pos, End: min64(r.Start, end)})
		}
		if r.End > pos {
			pos = r.End
		}
		if pos >= end {
			break
		}
	}
	if pos < end {
		out = append(out, byteRange{Start: pos, End: end})
	}
	return out
}

func rangesLen(ranges []byteRange) int64 {
	var n int64
	for _, r := range normalizeRanges(ranges, math.MaxInt64) {
		if r.End > r.Start {
			n += r.End - r.Start
		}
	}
	return n
}

func rangesValid(ranges []byteRange, size int64) bool {
	prev := int64(0)
	first := true
	for _, r := range ranges {
		if r.Start < 0 || r.End < r.Start || r.End > size {
			return false
		}
		if !first && r.Start < prev {
			return false
		}
		prev = r.End
		first = false
	}
	return true
}

func cloneRanges(ranges []byteRange) []byteRange {
	if len(ranges) == 0 {
		return nil
	}
	out := make([]byteRange, len(ranges))
	copy(out, ranges)
	return out
}

func addChecksum(sums []blockChecksum, next blockChecksum) []blockChecksum {
	out := sums[:0]
	for _, s := range sums {
		if s.Start == next.Start && s.End == next.End {
			continue
		}
		out = append(out, s)
	}
	out = append(out, next)
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

func rangeKey(start, end int64) string { return fmt.Sprintf("%d-%d", start, end) }

func alignDown(n, block int64) int64 {
	if block <= 0 {
		return n
	}
	return n - n%block
}

func roundUp(n, block int64) int64 {
	if block <= 0 || n%block == 0 {
		return n
	}
	return n + block - n%block
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func dataFileSize(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

func isTempFile(path string) bool {
	base := filepath.Base(path)
	return strings.Contains(base, ".tmp") || strings.HasSuffix(base, "~") || strings.HasSuffix(base, ".lock")
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sumEntryBytes(entries []EntryInfo) int64 {
	var n int64
	for _, e := range entries {
		n += e.DataBytes
	}
	return n
}

func diskFree(path string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return -1
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

func joinErrors(errs ...error) error {
	var parts []string
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return errors.New(strings.Join(parts, "; "))
}
