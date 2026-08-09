package log

// Trace is the no-op internal tracing hook used by copied reader code.
func Trace(any, string, ...any) func(string, ...any) {
	return func(string, ...any) {}
}
