package presence

import (
	"context"
	"time"
)

// Tiny ctx helpers funnelled here so engine_test.go can stay focused
// on scenarios without an extra import.

type (
	ctx    = context.Context //nolint:unused // used by Run-cancellation test
	cancel = context.CancelFunc
)

func newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
