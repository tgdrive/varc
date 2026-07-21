package varc

// adaptiveReadState belongs to one Reader. Cache coverage remains shared, but
// growth decisions must not leak between a sequential player and a random
// reader of the same object.
type adaptiveReadState struct {
	chunkSize       int64
	lastReadEnd     int64
	sequentialReads int
	initialized     bool
}

func (r *Reader) beginAdaptiveAccess(off int64) {
	r.adaptiveMu.Lock()
	defer r.adaptiveMu.Unlock()
	if !r.adaptive.initialized {
		r.adaptive = adaptiveReadState{chunkSize: r.cache.chunkSize, lastReadEnd: off, initialized: true}
		return
	}
	if off != r.adaptive.lastReadEnd {
		r.resetAdaptiveLocked(off)
	}
}

func (r *Reader) finishAdaptiveAccess(off, n int64) {
	if n <= 0 {
		return
	}
	r.adaptiveMu.Lock()
	defer r.adaptiveMu.Unlock()
	if !r.adaptive.initialized {
		r.adaptive = adaptiveReadState{chunkSize: r.cache.chunkSize, initialized: true}
	}
	if off == r.adaptive.lastReadEnd {
		r.adaptive.sequentialReads++
	} else {
		r.adaptive.sequentialReads = 1
		r.adaptive.chunkSize = r.cache.chunkSize
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
	defer r.adaptiveMu.Unlock()
	r.resetAdaptiveLocked(off)
}

func (r *Reader) resetAdaptiveLocked(off int64) {
	r.adaptive = adaptiveReadState{chunkSize: r.cache.chunkSize, lastReadEnd: off, initialized: true}
}

func (r *Reader) adaptiveChunkSize() int64 {
	r.adaptiveMu.Lock()
	defer r.adaptiveMu.Unlock()
	if r.adaptive.chunkSize <= 0 {
		return r.cache.chunkSize
	}
	return r.adaptive.chunkSize
}

func (r *Reader) ensureAdaptiveTasksLocked(st *entryState, start, end, fileSize int64) {
	chunkSize := r.adaptiveChunkSize()
	missingStart, missingEnd, ok := firstMissingRange(scheduledCoverageLocked(st), start, end)
	if ok {
		r.cache.ensureTaskLocked(st, r.src, missingStart, missingEnd, chunkSize, priorityBlocking)
	}
	preloadStart := max64(end, saturatingAdd(start, chunkSize))
	for i := 0; i < r.cache.preloadChunks && preloadStart < fileSize; i++ {
		preloadEnd := min64(fileSize, saturatingAdd(preloadStart, chunkSize))
		priority := prioritySpeculativePreload
		if i == 0 {
			priority = priorityImmediatePreload
		}
		r.cache.ensureTaskLocked(st, r.src, preloadStart, preloadEnd, chunkSize, priority)
		preloadStart = preloadEnd
	}
}
