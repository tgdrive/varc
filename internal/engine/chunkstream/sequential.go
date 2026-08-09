package chunkstream

import (
	"context"
	"io"
	"sync"

	"github.com/tgdrive/varc/internal/engine/objectio"
)

type sequential struct {
	ctx              context.Context
	mu               sync.Mutex
	o                objectio.Object
	rc               io.ReadCloser
	offset           int64
	chunkOffset      int64
	chunkSize        int64
	initialChunkSize int64
	maxChunkSize     int64
	customChunkSize  bool
	closed           bool
}

func newSequential(ctx context.Context, o objectio.Object, initialChunkSize int64, maxChunkSize int64) Reader {
	return &sequential{
		ctx:              ctx,
		o:                o,
		offset:           -1,
		chunkSize:        initialChunkSize,
		initialChunkSize: initialChunkSize,
		maxChunkSize:     maxChunkSize,
	}
}

func (cr *sequential) Read(p []byte) (n int, err error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.closed {
		return 0, ErrorFileClosed
	}
	for reqSize := int64(len(p)); reqSize > 0; reqSize = int64(len(p)) {
		chunkEnd := cr.chunkOffset + cr.chunkSize
		switch {
		case cr.chunkSize > 0 && cr.offset == chunkEnd:
			cr.chunkOffset = cr.offset
			if cr.customChunkSize {
				cr.customChunkSize = false
				cr.chunkSize = cr.initialChunkSize
			} else {
				cr.chunkSize *= 2
				if cr.chunkSize > cr.maxChunkSize && cr.maxChunkSize != -1 {
					cr.chunkSize = cr.maxChunkSize
				}
			}
			chunkEnd = cr.chunkOffset + cr.chunkSize
			fallthrough
		case cr.offset == -1:
			err = cr.openRange()
			if err != nil {
				return
			}
		}
		var buf []byte
		chunkRest := chunkEnd - cr.offset
		if reqSize > chunkRest && cr.chunkSize > 0 {
			buf, p = p[0:chunkRest], p[chunkRest:]
		} else {
			buf, p = p, nil
		}
		var rn int
		rn, err = io.ReadFull(cr.rc, buf)
		n += rn
		cr.offset += int64(rn)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				err = io.EOF
			}
			return
		}
	}
	return n, nil
}

func (cr *sequential) Close() error {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.closed {
		return ErrorFileClosed
	}
	cr.closed = true
	return cr.resetReader(nil, 0)
}

func (cr *sequential) Seek(offset int64, whence int) (int64, error) {
	return cr.RangeSeek(context.TODO(), offset, whence, -1)
}

func (cr *sequential) RangeSeek(ctx context.Context, offset int64, whence int, length int64) (int64, error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.closed {
		return 0, ErrorFileClosed
	}
	size := cr.o.Size()
	switch whence {
	case io.SeekStart:
		cr.offset = 0
	case io.SeekEnd:
		if size < 0 {
			return 0, ErrorInvalidSeek
		}
		cr.offset = size
	}
	cr.chunkOffset = cr.offset + offset
	cr.offset = -1
	if length > 0 {
		cr.customChunkSize = true
		cr.chunkSize = length
	} else {
		cr.chunkSize = cr.initialChunkSize
	}
	if cr.chunkOffset < 0 || cr.chunkOffset >= size {
		cr.chunkOffset = 0
		return 0, ErrorInvalidSeek
	}
	return cr.chunkOffset, nil
}

func (cr *sequential) Open() (Reader, error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.rc != nil && cr.offset != -1 {
		return cr, nil
	}
	return cr, cr.openRange()
}

func (cr *sequential) openRange() error {
	offset, length := cr.chunkOffset, cr.chunkSize
	if cr.closed {
		return ErrorFileClosed
	}
	if rs, ok := cr.rc.(objectio.RangeSeeker); ok {
		n, err := rs.RangeSeek(cr.ctx, offset, io.SeekStart, length)
		if err == nil && n == offset {
			cr.offset = offset
			return nil
		}
	}
	var rc io.ReadCloser
	var err error
	if length <= 0 {
		if offset == 0 {
			rc, err = cr.o.Open(cr.ctx, nil)
		} else {
			rc, err = cr.o.Open(cr.ctx, &objectio.Span{Start: offset, End: -1})
		}
	} else {
		rc, err = cr.o.Open(cr.ctx, &objectio.Span{Start: offset, End: offset + length - 1})
	}
	if err != nil {
		return err
	}
	return cr.resetReader(rc, offset)
}

func (cr *sequential) resetReader(rc io.ReadCloser, offset int64) error {
	if cr.rc != nil {
		if err := cr.rc.Close(); err != nil {
			return err
		}
	}
	cr.rc = rc
	cr.offset = offset
	return nil
}

var _ Reader = (*sequential)(nil)
