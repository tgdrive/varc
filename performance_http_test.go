package varc

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corevarc "github.com/tgdrive/varc/varc"
)

func TestColdConcurrentHTTPOverhead(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("wall-clock performance test requires a normal non-race run")
	}
	const (
		concurrency = 8
		objectSize  = 4 << 20
		writeSize   = 64 << 10
		trials      = 5
	)

	var requests atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span, err := parseSingleRange(r.Header.Get("Range"), objectSize)
		if err != nil {
			writeRangeNotSatisfiable(w, objectSize)
			return
		}
		requests.Add(1)
		w.Header().Set("Content-Length", fmt.Sprint(span.Length()))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", span.Start, span.End-1, objectSize))
		w.WriteHeader(http.StatusPartialContent)
		block := make([]byte, writeSize)
		for remaining := span.Length(); remaining > 0; {
			n := minInt64(int64(len(block)), remaining)
			if _, err := w.Write(block[:n]); err != nil {
				return
			}
			remaining -= int64(n)
			time.Sleep(250 * time.Microsecond)
		}
	}))
	defer origin.Close()

	transport := &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}

	// Warm DNS, connection setup, and the server before collecting samples.
	if err := runDirectConcurrentReads(context.Background(), t.TempDir(), client, origin.URL, 1, objectSize); err != nil {
		t.Fatal(err)
	}
	requests.Store(0)

	directSamples := make([]time.Duration, 0, trials)
	varcSamples := make([]time.Duration, 0, trials)
	for trial := 0; trial < trials; trial++ {
		start := time.Now()
		if err := runDirectConcurrentReads(context.Background(), t.TempDir(), client, origin.URL, concurrency, objectSize); err != nil {
			t.Fatal(err)
		}
		directSamples = append(directSamples, time.Since(start))

		start = time.Now()
		if err := runVARCConcurrentReads(context.Background(), t.TempDir(), client, origin.URL, concurrency, objectSize); err != nil {
			t.Fatal(err)
		}
		varcSamples = append(varcSamples, time.Since(start))
	}

	directMedian := medianDuration(directSamples)
	varcMedian := medianDuration(varcSamples)
	overhead := varcMedian - directMedian
	ratio := float64(varcMedian) / float64(directMedian)
	totalMiB := float64(concurrency*objectSize) / (1 << 20)
	directMiBPS := totalMiB / directMedian.Seconds()
	varcMiBPS := totalMiB / varcMedian.Seconds()
	t.Logf("direct median=%s throughput=%.1f MiB/s; varc cold median=%s throughput=%.1f MiB/s; overhead=%s (%.2fx)", directMedian, directMiBPS, varcMedian, varcMiBPS, overhead, ratio)

	wantRequests := int64(trials * concurrency * 2)
	if got := requests.Load(); got != wantRequests {
		t.Fatalf("origin range requests=%d, want %d", got, wantRequests)
	}
	// Package-level test parallelism and filesystem scheduling can add tens of
	// milliseconds of noise. A serialized regression would approach concurrency
	// times the direct duration, so this still leaves a wide detection margin.
	allowed := maxDuration(100*time.Millisecond, directMedian)
	if overhead > allowed {
		t.Fatalf("VARC cold-path overhead %s exceeds allowance %s (direct=%s varc=%s)", overhead, allowed, directMedian, varcMedian)
	}
}

func runDirectConcurrentReads(ctx context.Context, outputDir string, client *http.Client, baseURL string, concurrency int, size int64) error {
	return runConcurrent(concurrency, func(i int) error {
		src := &HTTPRangeSource{Client: client, URL: fmt.Sprintf("%s/object/%d", baseURL, i), ValidateSize: size}
		body, err := src.OpenRange(ctx, 0, size-1)
		if err != nil {
			return err
		}
		defer body.Close()
		output, err := os.Create(filepath.Join(outputDir, fmt.Sprintf("object-%d", i)))
		if err != nil {
			return err
		}
		n, copyErr := io.CopyBuffer(output, body, make([]byte, defaultResponseBuffer))
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n != size {
			return fmt.Errorf("direct object %d read %d of %d bytes", i, n, size)
		}
		return nil
	})
}

func runVARCConcurrentReads(ctx context.Context, cacheDir string, client *http.Client, baseURL string, concurrency int, size int64) error {
	opt := corevarc.DefaultOptions()
	opt.CacheDir = cacheDir
	opt.ChunkSize = size
	opt.ChunkSizeLimit = size * int64(concurrency)
	opt.PreloadChunks = -1
	opt.NoBackground = true
	opt.ReadRetryCount = 0
	cache, err := corevarc.New(ctx, opt)
	if err != nil {
		return err
	}
	defer cache.Close()

	return runConcurrent(concurrency, func(i int) error {
		src := &HTTPRangeSource{Client: client, URL: fmt.Sprintf("%s/object/%d", baseURL, i), ValidateSize: size}
		reader, err := cache.Open(ctx, fmt.Sprintf("object-%d", i), size, src)
		if err != nil {
			return err
		}
		defer reader.Close()
		n, err := io.CopyBuffer(io.Discard, reader, make([]byte, defaultResponseBuffer))
		if err != nil {
			return err
		}
		if n != size {
			return fmt.Errorf("VARC object %d read %d of %d bytes", i, n, size)
		}
		return nil
	})
}

func runConcurrent(concurrency int, fn func(int) error) error {
	start := make(chan struct{})
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs <- fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func medianDuration(samples []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

func minInt64(a, b int64) int {
	if a < b {
		return int(a)
	}
	return int(b)
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
