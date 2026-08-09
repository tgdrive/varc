package objectio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
)

var errInjectedRead = errors.New("injected read failure")

type retryTestObject struct {
	data     []byte
	failures []retryFailure
	mu       sync.Mutex
	spans    []Span
	opens    int
}

type retryFailure struct {
	after int
	err   error
}

func (o *retryTestObject) Size() int64 { return int64(len(o.data)) }

func (o *retryTestObject) Open(_ context.Context, span *Span) (io.ReadCloser, error) {
	o.mu.Lock()
	attempt := o.opens
	o.opens++
	start := int64(0)
	end := int64(len(o.data))
	if span != nil {
		o.spans = append(o.spans, *span)
		start = span.Start
		if span.End >= 0 {
			end = span.End + 1
		}
	} else {
		o.spans = append(o.spans, Span{Start: 0, End: -1})
	}
	var failure *retryFailure
	if attempt < len(o.failures) {
		copyFailure := o.failures[attempt]
		failure = &copyFailure
	}
	o.mu.Unlock()

	if end > int64(len(o.data)) {
		end = int64(len(o.data))
	}
	data := o.data[start:end]
	if failure == nil {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return io.NopCloser(&failAfterReader{data: data, remaining: failure.after, err: failure.err}), nil
}

func (o *retryTestObject) snapshot() ([]Span, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]Span(nil), o.spans...), o.opens
}

type failAfterReader struct {
	data      []byte
	remaining int
	err       error
	failed    bool
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.failed {
		return 0, r.err
	}
	if r.remaining <= 0 {
		r.failed = true
		return 0, r.err
	}
	n := min(len(p), len(r.data), r.remaining)
	copy(p, r.data[:n])
	r.data = r.data[n:]
	r.remaining -= n
	if r.remaining == 0 {
		r.failed = true
		return n, r.err
	}
	if len(r.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}

type terminalRetryError struct{ error }

func (terminalRetryError) NoLowLevelRetry() bool { return true }
func (err terminalRetryError) Unwrap() error     { return err.error }

func TestRetryReaderResumesAtConsumedOffset(t *testing.T) {
	src := &retryTestObject{
		data:     []byte("0123456789"),
		failures: []retryFailure{{after: 3, err: errInjectedRead}},
	}
	ctx := WithConfig(context.Background(), Config{LowLevelRetries: 10})
	r, err := OpenRetrying(ctx, src, &Span{Start: 2, End: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	buf := make([]byte, 6)
	n, err := r.Read(buf)
	if err != nil || n != len(buf) || string(buf) != "234567" {
		t.Fatalf("Read = %d, %v, %q", n, err, buf[:n])
	}
	spans, opens := src.snapshot()
	want := []Span{{Start: 2, End: 7}, {Start: 5, End: 7}}
	if opens != 2 || len(spans) != len(want) {
		t.Fatalf("opens/spans = %d/%+v, want 2/%+v", opens, spans, want)
	}
	for i := range want {
		if spans[i] != want[i] {
			t.Fatalf("span %d = %+v, want %+v", i, spans[i], want[i])
		}
	}
}

func TestRetryReaderStopsAfterMaxTries(t *testing.T) {
	src := &retryTestObject{
		data: []byte("0123456789"),
		failures: []retryFailure{
			{after: 2, err: errInjectedRead},
			{after: 1, err: errInjectedRead},
		},
	}
	ctx := WithConfig(context.Background(), Config{LowLevelRetries: 2})
	r, err := OpenRetrying(ctx, src, &Span{Start: 0, End: 9})
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 10)
	n, err := r.Read(buf)
	if !errors.Is(err, errInjectedRead) || n != 3 || string(buf[:n]) != "012" {
		t.Fatalf("first Read = %d, %v, %q", n, err, buf[:n])
	}
	if n, err = r.Read(buf); !errors.Is(err, errTooManyRetryOpens) || n != 0 {
		t.Fatalf("second Read = %d, %v, want too-many-retries", n, err)
	}
	spans, opens := src.snapshot()
	if opens != 2 || len(spans) != 2 || spans[0] != (Span{Start: 0, End: 9}) || spans[1] != (Span{Start: 2, End: 9}) {
		t.Fatalf("opens/spans = %d/%+v", opens, spans)
	}
	if err := r.Close(); !errors.Is(err, errRetryReaderClosed) {
		t.Fatalf("Close after exhausted retries = %v", err)
	}
}

func TestRetryReaderHonorsTerminalErrorMarker(t *testing.T) {
	terminal := terminalRetryError{errInjectedRead}
	src := &retryTestObject{
		data:     []byte("0123456789"),
		failures: []retryFailure{{after: 2, err: terminal}},
	}
	ctx := WithConfig(context.Background(), Config{LowLevelRetries: 10})
	r, err := OpenRetrying(ctx, src, &Span{Start: 0, End: 9})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	buf := make([]byte, 10)
	n, err := r.Read(buf)
	if !errors.Is(err, errInjectedRead) || n != 2 {
		t.Fatalf("Read = %d, %v", n, err)
	}
	_, opens := src.snapshot()
	if opens != 1 {
		t.Fatalf("opens = %d, want 1 for terminal error", opens)
	}
}
