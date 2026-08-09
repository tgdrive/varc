package chunkedreader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"vfs-cache/internal/cachecore/asyncreader"
	"vfs-cache/internal/cachecore/fs"
	"vfs-cache/internal/cachecore/hash"
	"vfs-cache/internal/cachecore/log"
	"vfs-cache/internal/cachecore/multipart"
	"vfs-cache/internal/cachecore/operations"
	"vfs-cache/internal/cachecore/pool"
)

type parallel struct {
	ctx       context.Context
	o         fs.Object
	mu        sync.Mutex
	endStream int64
	offset    int64
	chunkSize int64
	nstreams  int
	streams   []*stream
	closed    bool
}

type stream struct {
	cr        *parallel
	ctx       context.Context
	cancel    func()
	rc        io.ReadCloser
	offset    int64
	size      int64
	readBytes int64
	rw        *pool.RW
	err       chan error
	name      string
}

func (cr *parallel) newStream(ctx context.Context, offset, size int64) (s *stream, err error) {
	ctx, cancel := context.WithCancel(ctx)
	rw := multipart.NewRW()
	s = &stream{cr: cr, ctx: ctx, cancel: cancel, offset: offset, size: size, rw: rw, err: make(chan error, 1)}
	s.name = fmt.Sprintf("stream(%d,%d,%p)", s.offset, s.size, s)
	go s.readFrom(ctx)
	return s, nil
}

func (s *stream) readFrom(ctx context.Context) {
	fs.Debugf(s.cr.o, "%s: open", s.name)
	rc, err := operations.Open(ctx, s.cr.o,
		&fs.HashesOption{Hashes: hash.Set(hash.None)},
		&fs.RangeOption{Start: s.offset, End: s.offset + s.size - 1})
	if err != nil {
		s.err <- fmt.Errorf("parallel chunked reader: failed to open stream at %d size %d: %w", s.offset, s.size, err)
		return
	}
	s.rc = rc
	fs.Debugf(s.cr.o, "%s: readfrom started", s.name)
	_, err = s.rw.ReadFrom(s.rc)
	fs.Debugf(s.cr.o, "%s: readfrom finished (%d bytes): %v", s.name, s.rw.Size(), err)
	s.err <- err
}

func (s *stream) eof() bool { return s.readBytes >= s.size }

func (s *stream) read(p []byte) (n int, err error) {
	defer log.Trace(s.cr.o, "%s: Read len(p)=%d", s.name, len(p))("n=%d, err=%v", &n, &err)
	if len(p) == 0 {
		return n, nil
	}
	for {
		nn, readErr := s.rw.Read(p[n:])
		fs.Debugf(s.cr.o, "%s: rw.Read nn=%d, err=%v", s.name, nn, readErr)
		s.readBytes += int64(nn)
		n += nn
		if readErr != nil && readErr != io.EOF {
			return n, readErr
		}
		if s.eof() {
			return n, io.EOF
		}
		if n >= len(p) {
			break
		}
		s.rw.WaitWrite(s.ctx)
	}
	return n, nil
}

func orErr(perr *error, newErr error) {
	if *perr == nil {
		*perr = newErr
	}
}

func (s *stream) close() (err error) {
	defer log.Trace(s.cr.o, "%s: close", s.name)("err=%v", &err)
	s.cancel()
	err = <-s.err
	orErr(&err, s.rw.Close())
	if s.rc != nil {
		orErr(&err, s.rc.Close())
	}
	if errors.Is(err, context.Canceled) {
		return asyncreader.ErrorStreamAbandoned
	}
	if err != nil && err != io.EOF {
		return fmt.Errorf("parallel chunked reader: failed to read stream at %d size %d: %w", s.offset, s.size, err)
	}
	return nil
}

func newParallel(ctx context.Context, o fs.Object, chunkSize int64, streams int) ChunkedReader {
	if chunkSize < 0 {
		chunkSize = multipart.BufferSize
	}
	newChunkSize := multipart.BufferSize * (chunkSize / multipart.BufferSize)
	if newChunkSize < chunkSize {
		newChunkSize += multipart.BufferSize
	}
	fs.Debugf(o, "newParallel chunkSize=%d, streams=%d", chunkSize, streams)
	return &parallel{ctx: ctx, o: o, offset: 0, chunkSize: newChunkSize, nstreams: streams}
}

func (cr *parallel) _open() (err error) {
	size := cr.o.Size()
	if size < 0 {
		return fmt.Errorf("parallel chunked reader: can't use multiple threads for unknown sized object %q", cr.o)
	}
	if cr.endStream >= size {
		return nil
	}
	for i := len(cr.streams); i < cr.nstreams; i++ {
		chunkSize := cr.chunkSize
		newEndStream := cr.endStream + chunkSize
		if newEndStream > size {
			chunkSize = size - cr.endStream
			newEndStream = cr.endStream + chunkSize
		}
		s, err := cr.newStream(cr.ctx, cr.endStream, chunkSize)
		if err != nil {
			return err
		}
		cr.streams = append(cr.streams, s)
		cr.endStream = newEndStream
		if cr.endStream >= size {
			break
		}
	}
	return nil
}

func (cr *parallel) _popStream() (err error) {
	defer log.Trace(cr.o, "streams=%+v", cr.streams)("streams=%+v, err=%v", &cr.streams, &err)
	if len(cr.streams) == 0 {
		return nil
	}
	stream := cr.streams[0]
	err = stream.close()
	cr.streams[0] = nil
	cr.streams = cr.streams[1:]
	return err
}

func (cr *parallel) _popStreams() (err error) {
	defer log.Trace(cr.o, "streams=%+v", cr.streams)("streams=%+v, err=%v", &cr.streams, &err)
	for len(cr.streams) > 0 {
		orErr(&err, cr._popStream())
	}
	cr.streams = nil
	return err
}

func (cr *parallel) Read(p []byte) (n int, err error) {
	defer log.Trace(cr.o, "Read len(p)=%d", len(p))("n=%d, err=%v", &n, &err)
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.closed {
		return 0, ErrorFileClosed
	}
	for n < len(p) {
		err = cr._open()
		if err != nil {
			return n, err
		}
		if len(cr.streams) == 0 {
			return n, io.EOF
		}
		stream := cr.streams[0]
		nn, readErr := stream.read(p[n:])
		n += nn
		cr.offset += int64(nn)
		if readErr == io.EOF {
			err = cr._popStream()
			if err != nil {
				break
			}
		} else if readErr != nil {
			err = readErr
			break
		}
	}
	return n, err
}

func (cr *parallel) Close() error {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.closed {
		return ErrorFileClosed
	}
	cr.closed = true
	return cr._popStreams()
}

func (cr *parallel) Seek(offset int64, whence int) (int64, error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	fs.Debugf(cr.o, "parallel chunked reader: seek from %d to %d whence %d", cr.offset, offset, whence)
	if cr.closed {
		return 0, ErrorFileClosed
	}
	size := cr.o.Size()
	currentOffset := cr.offset
	switch whence {
	case io.SeekStart:
		currentOffset = 0
	case io.SeekEnd:
		currentOffset = size
	}
	newOffset := currentOffset + offset
	if newOffset < 0 || newOffset >= size {
		return 0, ErrorInvalidSeek
	}
	if newOffset == cr.offset {
		fs.Debugf(cr.o, "parallel chunked reader: seek pointer didn't move")
		return cr.offset, nil
	}
	cr.offset = newOffset
	for len(cr.streams) > 0 {
		stream := cr.streams[0]
		if newOffset >= stream.offset+stream.size {
			_ = cr._popStream()
		} else {
			break
		}
	}
	if len(cr.streams) == 0 {
		fs.Debugf(cr.o, "parallel chunked reader: no streams remain")
		cr.endStream = cr.offset
		return cr.offset, nil
	}
	stream := cr.streams[0]
	if newOffset < stream.offset {
		_ = cr._popStreams()
		fs.Debugf(cr.o, "parallel chunked reader: new offset is before current stream - ditch all")
		cr.endStream = cr.offset
		return cr.offset, nil
	}
	streamOffset := newOffset - stream.offset
	stream.readBytes = streamOffset
	fs.Debugf(cr.o, "parallel chunked reader: seek the current stream to %d", streamOffset)
	for stream.rw.Size() < streamOffset {
		stream.rw.WaitWrite(cr.ctx)
	}
	_, err := stream.rw.Seek(streamOffset, io.SeekStart)
	if err != nil {
		return cr.offset, fmt.Errorf("parallel chunked reader: failed to seek stream: %w", err)
	}
	return cr.offset, nil
}

func (cr *parallel) RangeSeek(ctx context.Context, offset int64, whence int, length int64) (int64, error) {
	return cr.Seek(offset, whence)
}

func (cr *parallel) Open() (ChunkedReader, error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	return cr, cr._open()
}

var _ ChunkedReader = (*parallel)(nil)
