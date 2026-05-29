package store

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// TestObservationsWriter_DropsOldestOnOverflow verifies that Submit
// matches mqtt.Publisher's drop-oldest semantics: when the buffer is
// full the freshest observation displaces the stalest one.
func TestObservationsWriter_DropsOldestOnOverflow(t *testing.T) {
	w := NewObservationsWriter(nil, ObservationsWriterOptions{
		BufferSize:    2,
		MaxBatch:      16,
		FlushInterval: time.Hour, // never auto-flush in this test
	}, discardLogger(t))

	// Fill past capacity without starting Run, so the buffer cannot drain.
	for i := 0; i < 5; i++ {
		w.Submit(Observation{Kind: "ibeacon", Key: "a", APMac: "m", RSSI: i})
	}

	// Drop count must be the three excess Submits.
	if got := w.Stats().Dropped; got != 3 {
		t.Errorf("Dropped: got %d want 3", got)
	}
	if got := w.Stats().Queued; got != 2 {
		t.Errorf("Queued: got %d want 2 (buffer cap)", got)
	}

	// The two surviving rows must be the newest two (RSSI 3 and 4) —
	// drop-oldest, not drop-newest.
	got := drainChan(w.in)
	if len(got) != 2 {
		t.Fatalf("drainChan: got %d rows", len(got))
	}
	if got[0].RSSI != 3 || got[1].RSSI != 4 {
		t.Errorf("surviving rows: got RSSI=%d,%d want 3,4 (drop-oldest)", got[0].RSSI, got[1].RSSI)
	}
}

// TestObservationsWriter_SubmitDropFiresHook is the load-bearing
// signal test: when Submit has to drop a row under saturation, the
// caller-supplied OnSubmitDropped MUST fire. Without this hook a
// Postgres stall is invisible to /metrics — the arrival-edge sighting
// vanishes silently. The hook receives the evicted row on the
// drop-oldest path and the incoming row when even that path can't
// make room.
func TestObservationsWriter_SubmitDropFiresHook(t *testing.T) {
	var (
		fired   atomic.Int32
		lastRow atomic.Pointer[Observation]
	)
	w := NewObservationsWriter(nil, ObservationsWriterOptions{
		BufferSize:    1,
		MaxBatch:      16,
		FlushInterval: time.Hour,
		OnSubmitDropped: func(o Observation) {
			fired.Add(1)
			c := o
			lastRow.Store(&c)
		},
	}, discardLogger(t))

	// First Submit fills the 1-slot buffer; no drop yet.
	w.Submit(Observation{Kind: "ibeacon", Key: "a", APMac: "m", RSSI: 1})
	if fired.Load() != 0 {
		t.Fatalf("hook fired prematurely on first Submit")
	}
	// Second Submit must drop the oldest (RSSI=1) and pass it to the
	// hook, mirroring mqtt.Publisher.OnDropped semantics.
	w.Submit(Observation{Kind: "ibeacon", Key: "a", APMac: "m", RSSI: 2})
	if fired.Load() != 1 {
		t.Fatalf("hook fire count after eviction: got %d want 1", fired.Load())
	}
	if got := lastRow.Load(); got == nil || got.RSSI != 1 {
		t.Errorf("hook received wrong row on eviction: %+v", got)
	}
}

// TestObservationsWriter_StatsSnapshot smokes the Stats counters
// independently of the flush path.
func TestObservationsWriter_StatsSnapshot(t *testing.T) {
	w := NewObservationsWriter(nil, ObservationsWriterOptions{
		BufferSize: 8,
	}, discardLogger(t))

	st := w.Stats()
	if st.Cap != 8 || st.Queued != 0 || st.Dropped != 0 || st.Written != 0 {
		t.Errorf("initial Stats: %+v", st)
	}
	w.Submit(Observation{Kind: "ibeacon", Key: "a", APMac: "m"})
	if got := w.Stats().Queued; got != 1 {
		t.Errorf("after Submit Queued: got %d want 1", got)
	}
}

// TestObservationsWriter_DrainOnShutdown verifies that ctx.Done causes
// Run to flush anything still in the buffer before returning, not just
// the in-flight batch slice. Uses a fake store via the inserter seam.
func TestObservationsWriter_DrainOnShutdown(t *testing.T) {
	fs := &fakeStoreInserter{}
	w := newWriterForTest(fs, ObservationsWriterOptions{
		BufferSize:    32,
		MaxBatch:      4,
		FlushInterval: time.Hour, // only ctx.Done or maxBatch can flush
	})

	// Submit 7 rows. With MaxBatch=4 and FlushInterval=1h, exactly one
	// flush of 4 should happen via the size trigger; the remaining 3
	// sit in the in-flight batch slice once they're pulled from w.in.
	// After ctx cancel, drainAndFlush must also commit those 3.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	for i := 0; i < 7; i++ {
		w.Submit(Observation{Kind: "ibeacon", Key: "a", APMac: "m", RSSI: i})
	}
	// Give the goroutine a beat to drain to the in-flight batch.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	if got := fs.totalRows.Load(); got != 7 {
		t.Errorf("rows persisted across shutdown: got %d want 7", got)
	}
}

// TestObservationsWriter_RetryOnTransientError verifies that a single
// transient error is retried and counted as written on success.
func TestObservationsWriter_RetryOnTransientError(t *testing.T) {
	fs := &fakeStoreInserter{}
	fs.failFirstN.Store(1)
	w := newWriterForTest(fs, ObservationsWriterOptions{
		BufferSize:    8,
		MaxBatch:      2,
		FlushInterval: time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	w.Submit(Observation{Kind: "ibeacon", Key: "a", APMac: "m"})
	w.Submit(Observation{Kind: "ibeacon", Key: "b", APMac: "m"})
	// Wait until the second attempt commits.
	deadline := time.After(2 * time.Second)
	for fs.totalRows.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("retry didn't commit; totalRows=%d", fs.totalRows.Load())
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
	cancel()
	<-done

	if got := w.Stats().Written; got != 2 {
		t.Errorf("Written: got %d want 2", got)
	}
	if got := w.Stats().BatchesDropped; got != 0 {
		t.Errorf("BatchesDropped: got %d want 0", got)
	}
}

// TestObservationsWriter_BatchDroppedAfterRetry verifies that a flush
// failing both the initial and retry attempts increments BatchesDropped
// and fires the onBatchDropped hook.
func TestObservationsWriter_BatchDroppedAfterRetry(t *testing.T) {
	fs := &fakeStoreInserter{} // never succeed
	fs.failFirstN.Store(99)
	var dropped atomic.Int64
	opts := ObservationsWriterOptions{
		BufferSize:     8,
		MaxBatch:       1,
		FlushInterval:  time.Hour,
		OnBatchDropped: func(rows int) { dropped.Add(int64(rows)) },
	}
	w := newWriterForTest(fs, opts)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	w.Submit(Observation{Kind: "ibeacon", Key: "a", APMac: "m"})
	deadline := time.After(2 * time.Second)
	for w.Stats().BatchesDropped < 1 {
		select {
		case <-deadline:
			t.Fatalf("BatchesDropped not incremented; stats=%+v", w.Stats())
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
	cancel()
	<-done

	if got := dropped.Load(); got != 1 {
		t.Errorf("OnBatchDropped rows: got %d want 1", got)
	}
}

// --- test helpers ---------------------------------------------------

// fakeStoreInserter satisfies the writer's store dependency without
// touching Postgres. The writer field is *Postgres in prod, so the
// test wires through newWriterForTest below to swap in this fake via
// an unexported override.
type fakeStoreInserter struct {
	totalRows  atomic.Int64
	failFirstN atomic.Int32
}

// failFirstN as a struct field expressed via atomic so tests can read
// it safely; the constructor literal above sets it directly because
// atomic.Int32 zero value is fine.
//
//nolint:unused // referenced via literal above
func (f *fakeStoreInserter) shouldFail() bool {
	n := f.failFirstN.Load()
	if n <= 0 {
		return false
	}
	f.failFirstN.Store(n - 1)
	return true
}

func (f *fakeStoreInserter) InsertObservations(_ context.Context, obs []Observation) error {
	if f.shouldFail() {
		return errFakeStoreTransient
	}
	f.totalRows.Add(int64(len(obs)))
	return nil
}

var errFakeStoreTransient = stringError("fake store transient failure")

type stringError string

func (e stringError) Error() string { return string(e) }

// newWriterForTest builds an ObservationsWriter wired against the fake
// inserter. Pure test seam — production code constructs via
// NewObservationsWriter against *Postgres.
func newWriterForTest(fs *fakeStoreInserter, opts ObservationsWriterOptions) *ObservationsWriter {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 1024
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 256
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	w := &ObservationsWriter{
		in:              make(chan Observation, opts.BufferSize),
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		maxBatch:        opts.MaxBatch,
		flushTick:       opts.FlushInterval,
		onBatchDropped:  opts.OnBatchDropped,
		onSubmitDropped: opts.OnSubmitDropped,
	}
	w.testInserter = fs
	return w
}

// drainChan empties a chan Observation non-destructively for inspection.
func drainChan(ch chan Observation) []Observation {
	var out []Observation
	for {
		select {
		case o := <-ch:
			out = append(out, o)
		default:
			return out
		}
	}
}

func discardLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
