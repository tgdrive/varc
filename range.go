package varc

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type byteSpan struct {
	Start   int64
	End     int64 // exclusive
	Size    int64
	Partial bool
}

func (s byteSpan) Length() int64 {
	if s.End <= s.Start {
		return 0
	}
	return s.End - s.Start
}

func fullSpan(size int64) byteSpan {
	return byteSpan{Start: 0, End: size, Size: size}
}

func parseSingleRange(raw string, size int64) (byteSpan, error) {
	if size < 0 {
		return byteSpan{}, fmt.Errorf("unknown size")
	}
	if raw == "" {
		return fullSpan(size), nil
	}
	if !strings.HasPrefix(raw, "bytes=") {
		return byteSpan{}, fmt.Errorf("unsupported range unit")
	}
	spec := strings.TrimSpace(strings.TrimPrefix(raw, "bytes="))
	if spec == "" || strings.Contains(spec, ",") {
		return byteSpan{}, fmt.Errorf("only one byte range is supported")
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return byteSpan{}, fmt.Errorf("malformed range")
	}
	left, right := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	if left == "" {
		// suffix range: bytes=-500
		suffix, err := strconv.ParseInt(right, 10, 64)
		if err != nil || suffix <= 0 {
			return byteSpan{}, fmt.Errorf("bad suffix range")
		}
		if suffix > size {
			suffix = size
		}
		return byteSpan{Start: size - suffix, End: size, Size: size, Partial: true}, nil
	}
	start, err := strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 {
		return byteSpan{}, fmt.Errorf("bad range start")
	}
	if start >= size {
		return byteSpan{}, fmt.Errorf("range start beyond size")
	}
	end := size - 1
	if right != "" {
		end, err = strconv.ParseInt(right, 10, 64)
		if err != nil || end < start {
			return byteSpan{}, fmt.Errorf("bad range end")
		}
		if end >= size {
			end = size - 1
		}
	}
	return byteSpan{Start: start, End: end + 1, Size: size, Partial: true}, nil
}

// parseContentRange parses "bytes start-end/size".  Unknown total returns -1.
func parseContentRange(raw string) (start, end, total int64, ok bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(strings.ToLower(raw), "bytes ") {
		return 0, 0, 0, false
	}
	raw = strings.TrimSpace(raw[6:])
	parts := strings.SplitN(raw, "/", 2)
	if len(parts) != 2 {
		return 0, 0, 0, false
	}
	rangePart := parts[0]
	totalPart := parts[1]
	dash := strings.IndexByte(rangePart, '-')
	if dash < 0 {
		return 0, 0, 0, false
	}
	var err error
	start, err = strconv.ParseInt(rangePart[:dash], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	end, err = strconv.ParseInt(rangePart[dash+1:], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	if totalPart == "*" {
		total = -1
	} else {
		total, err = strconv.ParseInt(totalPart, 10, 64)
		if err != nil {
			return 0, 0, 0, false
		}
	}
	if start < 0 || end < start {
		return 0, 0, 0, false
	}
	return start, end, total, true
}

func rangeAllowedByIfRange(r *http.Request, etag string, modTime time.Time) bool {
	if r.Header.Get("Range") == "" {
		return true
	}
	raw := strings.TrimSpace(r.Header.Get("If-Range"))
	if raw == "" {
		return true
	}
	if strings.HasPrefix(raw, "\"") || strings.HasPrefix(raw, "W/\"") {
		return etag != "" && raw == etag
	}
	if t, err := http.ParseTime(raw); err == nil {
		if modTime.IsZero() {
			return false
		}
		return !modTime.After(t)
	}
	return false
}

func isNotModified(r *http.Request, etag string, modTime time.Time) bool {
	if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && etag != "" {
		for _, token := range strings.Split(inm, ",") {
			token = strings.TrimSpace(token)
			if token == "*" || token == etag {
				return true
			}
		}
	}
	if ims := strings.TrimSpace(r.Header.Get("If-Modified-Since")); ims != "" && !modTime.IsZero() {
		if t, err := http.ParseTime(ims); err == nil {
			return !modTime.After(t)
		}
	}
	return false
}
