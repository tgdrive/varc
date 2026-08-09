package source

import "context"

type requestHeadersKey struct{}

// WithRequestHeaders attaches a defensive copy of request headers to ctx for
// source implementations which need per-request upstream metadata such as
// authorization, cookies, or tenant headers.
func WithRequestHeaders(ctx context.Context, headers map[string][]string) context.Context {
	if ctx == nil || len(headers) == 0 {
		return ctx
	}
	return context.WithValue(ctx, requestHeadersKey{}, cloneHeaders(headers))
}

// RequestHeaders returns a defensive copy of request headers attached to ctx.
func RequestHeaders(ctx context.Context) map[string][]string {
	if ctx == nil {
		return nil
	}
	headers, _ := ctx.Value(requestHeadersKey{}).(map[string][]string)
	return cloneHeaders(headers)
}

func cloneHeaders(headers map[string][]string) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(headers))
	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}
