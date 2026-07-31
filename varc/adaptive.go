package varc

// adaptiveReadState belongs to one Reader. Cache coverage remains shared, but
// growth decisions and chunk-window alignment must not leak between readers.
type adaptiveReadState struct {
	chunkSize       int64
	windowStart     int64
	windowEnd       int64
	lastReadEnd     int64
	sequentialReads int
	initialized     bool
}

func (r *Reader) beginAdaptiveAccess(off int64) {
	r.adaptiveMu.Lock()
	if !r.adaptive.initialized {
		r.resetAdaptiveLocked(off)
		r.adaptiveMu.Unlock()
		return
	}
	if off != r.adaptive.lastReadEnd {
		r.resetAdaptiveLocked(off)
		r.adaptiveMu.Unlock()
		r.cancelStalePreloads()
		return
	}
	for off >= r.adaptive.windowEnd {
		r.adaptive.windowStart = r.adaptive.windowEnd
		r.adaptive.windowEnd = saturatingAdd(r.adaptive.windowStart, r.adaptive.chunkSize)
	}
	r.adaptiveMu.Unlock()
}

func (r *Reader) finishAdaptiveAccess(off, n int64) {
	if n <= 0 {
		return
	}
	r.adaptiveMu.Lock()
	defer r.adaptiveMu.Unlock()
	if !r.adaptive.initialized {
		r.resetAdaptiveLocked(off)
	}
	if off == r.adaptive.lastReadEnd {
		r.adaptive.sequentialReads++
	} else {
		r.adaptive.sequentialReads = 1
		r.adaptive.chunkSize = r.cache.chunkSize
		r.adaptive.windowStart = off
		r.adaptive.windowEnd = saturatingAdd(off, r.cache.chunkSize)
	}
	r.adaptive.lastReadEnd = off + n

	base := r.cache.chunkSize
	limit := r.cache.chunkSizeLimit
	if limit <= 0 {
		limit = base
	}
	var target int64
	switch {
	case r.adaptive.sequentialReads >= 4:
		target = saturatingMul(4, base)
	case r.adaptive.sequentialReads >= 2:
		target = saturatingMul(2, base)
	default:
		target = base
	}
	if target > limit {
		target = limit
	}
	r.adaptive.chunkSize = target
}

func (r *Reader) resetAdaptive(off int64) {
	r.adaptiveMu.Lock()
	r.resetAdaptiveLocked(off)
	r.adaptiveMu.Unlock()
	r.cancelStaleTasks()
}

func (r *Reader) cancelStaleTasks() {
	r.cancelOwnedTasks(true)
}

func (r *Reader) cancelStalePreloads() {
	r.cancelOwnedTasks(false)
}

func (r *Reader) cancelOwnedTasks(includeBlocking bool) {
	if r == nil || r.cache == nil || r.state == nil {
		return
	}
	r.state.mu.Lock()
	r.cache.cancelReaderTasksLocked(r.state, r, includeBlocking)
	r.state.mu.Unlock()
}

func (r *Reader) resetAdaptiveLocked(off int64) {
	chunkSize := r.cache.chunkSize
	r.adaptive = adaptiveReadState{
		chunkSize:   chunkSize,
		windowStart: off,
		windowEnd:   saturatingAdd(off, chunkSize),
		lastReadEnd: off,
		initialized: true,
	}
}

func (r *Reader) adaptiveWindow() (chunkSize, windowEnd int64) {
	r.adaptiveMu.Lock()
	defer r.adaptiveMu.Unlock()
	if !r.adaptive.initialized {
		r.resetAdaptiveLocked(0)
	}
	chunkSize = r.adaptive.chunkSize
	if chunkSize <= 0 {
		chunkSize = r.cache.chunkSize
	}
	windowEnd = r.adaptive.windowEnd
	if windowEnd <= r.adaptive.windowStart {
		windowEnd = saturatingAdd(r.adaptive.windowStart, chunkSize)
	}
	return chunkSize, windowEnd
}

func (r *Reader) adaptiveChunkSize() int64 {
	chunkSize, _ := r.adaptiveWindow()
	return chunkSize
}

func (r *Reader) ensureAdaptiveTasksLocked(st *entryState, start, end, fileSize int64) {
	chunkSize, _ := r.adaptiveWindow()
	r.cache.claimBlockingTaskLocked(st, start, end, r)
	missingStart, missingEnd, ok := firstMissingRange(scheduledCoverageLocked(st), start, end)
	if ok {
		r.cache.ensureTaskLocked(st, r.src, missingStart, missingEnd, chunkSize, priorityBlocking, r)
	}
	r.ensureAdaptivePreloadsLocked(st, end, fileSize)
}

func (r *Reader) ensureAdaptivePreloadsLocked(st *entryState, readEnd, fileSize int64) {
	if r.src == nil || r.cacheOnly || r.cache.preloadChunks <= 0 {
		return
	}
	chunkSize, windowEnd := r.adaptiveWindow()
	preloadStart := windowEnd
	for preloadStart < readEnd {
		preloadStart = saturatingAdd(preloadStart, chunkSize)
	}
	for i := 0; i < r.cache.preloadChunks && preloadStart < fileSize; i++ {
		preloadEnd := min64(fileSize, saturatingAdd(preloadStart, chunkSize))
		priority := prioritySpeculativePreload
		if i == 0 {
			priority = priorityImmediatePreload
		}
		r.cache.ensurePreloadTaskLocked(st, r.src, preloadStart, preloadEnd, chunkSize, priority, r)
		preloadStart = preloadEnd
	}
}
