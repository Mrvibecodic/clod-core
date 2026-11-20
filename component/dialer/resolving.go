package dialer

import "context"

type resolvingKey struct{}

// WithResolving returns a context whose dials call begin before they look up
// the address they dial, and the function begin returns once the lookup is
// over.
func WithResolving(ctx context.Context, begin func() (end func())) context.Context {
	return context.WithValue(ctx, resolvingKey{}, begin)
}

// Resolving marks the start of an address lookup made for a dial with ctx;
// the caller calls the result when the lookup is over.
func Resolving(ctx context.Context) (end func()) {
	if begin, ok := ctx.Value(resolvingKey{}).(func() func()); ok {
		return begin()
	}
	return noResolving
}

func noResolving() {}
