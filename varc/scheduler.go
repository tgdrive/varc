package varc

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

type taskPriority uint8

const (
	priorityBlocking taskPriority = iota
	priorityImmediatePreload
	prioritySpeculativePreload
)

// scheduledCoverageLocked returns every byte range that is already durable,
// progressively readable, or owned by an active task. Callers must hold st.mu.
func scheduledCoverageLocked(st *entryState) []byteRange {
	available := append([]byteRange(nil), st.meta.Ranges...)
	for _, volatile := range st.volatile {
		available = addRange(available, volatile.Start, volatile.End)
	}
	for _, task := range st.tasks {
		if !task.done && task.err == nil {
			available = addRange(available, task.start, task.end)
		}
	}
	return available
}

func (c *Cache) ensureTaskLocked(st *entryState, src io.ReaderAt, start, end, chunkSize int64, priority taskPriority) {
	c.ensureOwnedTaskLocked(st, src, start, end, chunkSize, priority, nil)
}

func (c *Cache) ensurePreloadTaskLocked(st *entryState, src io.ReaderAt, start, end, chunkSize int64, priority taskPriority, owner *Reader) {
	c.ensureOwnedTaskLocked(st, src, start, end, chunkSize, priority, owner)
}

func (c *Cache) ensureOwnedTaskLocked(st *entryState, src io.ReaderAt, start, end, chunkSize int64, priority taskPriority, owner *Reader) {
	c.pruneTasksLocked(st)
	for _, task := range st.tasks {
		if !task.done && task.err == nil && task.start <= start && task.end >= end {
			if owner != nil && task.priority != priorityBlocking {
				if task.preloadOwners == nil {
					task.preloadOwners = make(map[*Reader]struct{})
				}
				task.preloadOwners[owner] = struct{}{}
			}
			c.promoteTask(task, priority)
			return
		}
	}
	if containsRange(scheduledCoverageLocked(st), start, end) {
		return
	}
	chunkStart := start
	if chunkSize <= 0 {
		chunkSize = c.chunkSize
	}
	chunkEnd := saturatingAdd(chunkStart, chunkSize)
	if chunkEnd > st.meta.Size {
		chunkEnd = st.meta.Size
	}
	key := rangeKey(chunkStart, chunkEnd)
	if existing := st.tasks[key]; existing != nil && !existing.done {
		return
	}
	taskCtx, cancel := context.WithCancel(c.ctx)
	t := &downloadTask{
		state:      st,
		cache:      c,
		src:        src,
		start:      chunkStart,
		end:        chunkEnd,
		key:        key,
		ctx:        taskCtx,
		cancel:     cancel,
		priority:   priority,
		generation: st.generation,
	}
	if owner != nil {
		t.preloadOwners = map[*Reader]struct{}{owner: {}}
	}
	st.tasks[key] = t
	c.schedulerMu.Lock()
	t.sequence = c.nextTaskSeq
	c.nextTaskSeq++
	c.waitingTasks = append(c.waitingTasks, t)
	if priority == priorityBlocking {
		active := c.activeTasks[st]
		if active != nil && active.priority != priorityBlocking {
			active.cancel()
		}
	}
	c.schedulerCond.Broadcast()
	c.schedulerMu.Unlock()
	st.notifyLocked()
	c.wg.Add(1)
	go c.runDownloadTask(t)
}

func (c *Cache) cancelReaderPreloadsLocked(st *entryState, owner *Reader) {
	for _, task := range st.tasks {
		if task.done || task.priority == priorityBlocking || task.preloadOwners == nil {
			continue
		}
		delete(task.preloadOwners, owner)
		if len(task.preloadOwners) == 0 {
			task.cancel()
		}
	}
	c.schedulerMu.Lock()
	c.schedulerCond.Broadcast()
	c.schedulerMu.Unlock()
	st.notifyLocked()
}

func (c *Cache) runDownloadTask(t *downloadTask) {
	defer c.wg.Done()
	defer c.maybeForgetState(t.state)
	if err := c.acquireTask(t); err != nil {
		c.finishTask(t, err)
		return
	}
	defer c.releaseTask(t)
	src, ok := t.src.(RangeSource)
	if !ok {
		src = readerAtRangeSource{ReaderAt: t.src}
	}
	err := c.runStreamDownloadTask(t, src)
	if closeErr := c.closeTaskCacheFile(t); closeErr != nil && err == nil {
		err = closeErr
	}
	c.finishTask(t, err)
}

func (c *Cache) runStreamDownloadTask(t *downloadTask, src RangeSource) error {
	return c.downloadStreamChunk(t, src, t.start, t.end)
}

func (c *Cache) acquireTask(t *downloadTask) error {
	c.schedulerMu.Lock()
	defer c.schedulerMu.Unlock()
	for {
		if err := t.ctx.Err(); err != nil {
			c.removeWaitingTaskLocked(t)
			return err
		}
		if c.ctx.Err() != nil {
			c.removeWaitingTaskLocked(t)
			return ErrClosed
		}
		best := c.bestRunnableTaskLocked()
		if best == t {
			c.removeWaitingTaskLocked(t)
			c.activeTasks[t.state] = t
			t.started = true
			return nil
		}
		c.schedulerCond.Wait()
	}
}

func (c *Cache) releaseTask(t *downloadTask) {
	c.schedulerMu.Lock()
	if c.activeTasks[t.state] == t {
		delete(c.activeTasks, t.state)
	}
	c.schedulerCond.Broadcast()
	c.schedulerMu.Unlock()
}

func (c *Cache) promoteTask(t *downloadTask, priority taskPriority) {
	c.schedulerMu.Lock()
	if !t.started && priority < t.priority {
		t.priority = priority
		c.schedulerCond.Broadcast()
	}
	c.schedulerMu.Unlock()
}

func (c *Cache) bestRunnableTaskLocked() *downloadTask {
	var best *downloadTask
	for _, task := range c.waitingTasks {
		if c.activeTasks[task.state] != nil {
			continue
		}
		if best == nil || task.priority < best.priority || (task.priority == best.priority && task.sequence < best.sequence) {
			best = task
		}
	}
	return best
}

func (c *Cache) removeWaitingTaskLocked(target *downloadTask) {
	for i, task := range c.waitingTasks {
		if task == target {
			c.waitingTasks = append(c.waitingTasks[:i], c.waitingTasks[i+1:]...)
			return
		}
	}
}

func (c *Cache) downloadStreamChunk(t *downloadTask, src RangeSource, chunkStart, chunkEnd int64) error {
	offset := chunkStart
	bufferSize := int64(initialWriteBufferSize)
	var checksums []blockChecksum
	for attempt := 0; ; attempt++ {
		reserved := chunkEnd - offset
		if err := c.reserveInflight(t.ctx, reserved); err != nil {
			return err
		}

		attemptCtx, cancelAttempt := context.WithCancel(t.ctx)
		progress := make(chan struct{}, 1)
		watchdogStop := make(chan struct{})
		watchdogStopped := make(chan struct{})
		idleTimedOut := make(chan struct{})
		if c.readIdleTimeout > 0 {
			go func() {
				defer close(watchdogStopped)
				watchReadProgress(attemptCtx, cancelAttempt, c.readIdleTimeout, progress, watchdogStop, idleTimedOut)
			}()
		} else {
			close(watchdogStopped)
		}

		body, err := src.OpenRange(attemptCtx, offset, chunkEnd-1)
		c.metricSourceReads.Add(1)
		if err == nil && body == nil {
			err = errors.New("varc: range source returned a nil stream")
		}
		if err == nil {
			offset, bufferSize, checksums, err = c.consumeRangeStream(attemptCtx, t, body, offset, chunkEnd, bufferSize, checksums, progress)
			closeErr := body.Close()
			if err == nil && closeErr != nil {
				err = closeErr
			}
			if err == nil && offset < chunkEnd {
				err = io.ErrUnexpectedEOF
			}
		}
		if c.readIdleTimeout > 0 {
			close(watchdogStop)
		}
		cancelAttempt()
		<-watchdogStopped
		select {
		case <-idleTimedOut:
			err = fmt.Errorf("varc: source read idle for %s: %w", c.readIdleTimeout, context.DeadlineExceeded)
		default:
		}
		c.releaseInflight(reserved)
		if err == nil {
			return c.persistStreamProgress(t, chunkStart, offset, checksums)
		}
		if attempt >= c.readRetryCount || t.ctx.Err() != nil {
			if persistErr := c.persistStreamProgress(t, chunkStart, offset, checksums); persistErr != nil {
				return errors.Join(err, persistErr)
			}
			return err
		}
		if c.readRetryDelay > 0 {
			select {
			case <-time.After(time.Duration(attempt+1) * c.readRetryDelay):
			case <-t.ctx.Done():
				return t.ctx.Err()
			}
		}
	}
}

func watchReadProgress(ctx context.Context, cancel context.CancelFunc, timeout time.Duration, progress <-chan struct{}, stop <-chan struct{}, timedOut chan<- struct{}) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-progress:
			resetTimer(timer, timeout)
		case <-timer.C:
			// Prefer already queued progress over a simultaneous timer firing.
			select {
			case <-progress:
				timer.Reset(timeout)
				continue
			default:
			}
			close(timedOut)
			cancel()
			return
		case <-stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

func resetTimer(timer *time.Timer, timeout time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(timeout)
}

type progressReader struct {
	reader   io.Reader
	progress chan<- struct{}
}

func (r progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		select {
		case r.progress <- struct{}{}:
		default:
		}
	}
	return n, err
}

func (c *Cache) consumeRangeStream(ctx context.Context, t *downloadTask, body io.Reader, offset, streamEnd, bufferSize int64, checksums []blockChecksum, progress chan<- struct{}) (int64, int64, []blockChecksum, error) {
	buffer := make([]byte, maxWriteBufferSize)
	trackedBody := progressReader{reader: body, progress: progress}
	for offset < streamEnd {
		if err := ctx.Err(); err != nil {
			return offset, bufferSize, checksums, err
		}
		readSize := min64(bufferSize, streamEnd-offset)
		n, readErr := io.ReadFull(trackedBody, buffer[:readSize])
		if n == 0 {
			if readErr == nil {
				readErr = io.ErrNoProgress
			}
			return offset, bufferSize, checksums, readErr
		}
		buf := buffer[:n]
		end := offset + int64(n)
		c.metricSourceReadBytes.Add(int64(n))
		checksum := uint32(0)
		if c.verifyChecksum {
			checksum = crc32.ChecksumIEEE(buf)
		}
		t.state.mu.Lock()
		if t.state.removed || t.generation != t.state.generation {
			t.state.mu.Unlock()
			return offset, bufferSize, checksums, ErrClosed
		}
		if err := c.writeTaskCacheBlockLocked(t, buf, offset); err != nil {
			t.state.mu.Unlock()
			return offset, bufferSize, checksums, err
		}
		if c.verifyChecksum {
			checksums = append(checksums, blockChecksum{Start: offset, End: end, CRC32: checksum})
		}
		t.state.volatile = addRange(t.state.volatile, offset, end)
		if end < streamEnd || readErr != nil {
			t.state.notifyLocked()
		}
		t.state.mu.Unlock()
		offset = end
		if bufferSize < maxWriteBufferSize {
			bufferSize = min64(maxWriteBufferSize, bufferSize*2)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && offset < streamEnd {
				readErr = io.ErrUnexpectedEOF
			}
			return offset, bufferSize, checksums, readErr
		}
	}
	return offset, bufferSize, checksums, nil
}

func (c *Cache) persistStreamProgress(t *downloadTask, start, end int64, checksums []blockChecksum) error {
	if end <= start {
		return nil
	}
	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	if t.state.removed || t.generation != t.state.generation {
		return ErrClosed
	}
	oldRanges := append([]byteRange(nil), t.state.meta.Ranges...)
	oldChecksums := append([]blockChecksum(nil), t.state.meta.Checksums...)
	t.state.meta.Ranges = addRange(t.state.meta.Ranges, start, end)
	for _, checksum := range checksums {
		t.state.meta.Checksums = addChecksum(t.state.meta.Checksums, checksum)
	}
	t.state.meta.UpdatedAt = time.Now()
	if err := c.saveMetaLocked(t.state); err != nil {
		t.state.meta.Ranges = oldRanges
		t.state.meta.Checksums = oldChecksums
		t.state.volatile = subtractRange(t.state.volatile, start, end)
		t.state.notifyLocked()
		return err
	}
	t.state.volatile = subtractRange(t.state.volatile, start, end)
	t.state.notifyLocked()
	return nil
}
