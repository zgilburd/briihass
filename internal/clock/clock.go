// Package clock provides an injectable Clock interface so packages
// that do time arithmetic (notably internal/presence with EWMA decay)
// can be tested deterministically.
package clock

import (
	"sync"
	"time"
)

// Clock returns the current time. Production uses Real; tests use Fake.
type Clock interface {
	Now() time.Time
}

// Real wraps time.Now.
type Real struct{}

// Now returns time.Now.
func (Real) Now() time.Time { return time.Now() }

// Fake is a deterministic Clock that returns a value set explicitly
// by the test. Safe for concurrent use.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a Fake clock initialized to t.
func NewFake(t time.Time) *Fake {
	return &Fake{now: t}
}

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Set replaces the fake time.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	f.now = t
	f.mu.Unlock()
}

// Advance moves the fake time forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}
