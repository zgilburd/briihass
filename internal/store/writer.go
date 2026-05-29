package store

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// ObservationsWriter drains observations from a channel and flushes
// them in batches. Used by the ingest hot path so the request goroutine
// never blocks on a DB round trip. When the input channel is full,
// Submit drops the OLDEST queued row to make room for the new one —
// matching the MQTT publisher's policy so a stuck Postgres doesn't
// lose the freshest sighting (the one that matters for the arrival
// edge). If the channel still cannot accept after eviction (extreme
// contention where another producer refills the slot) the incoming
// row is dropped instead. Both paths increment Stats().Dropped and
// fire OnSubmitDropped.
type ObservationsWriter struct {
	store     *Postgres
	in        chan Observation
	logger    *slog.Logger
	maxBatch  int
	flushTick time.Duration

	dropped        atomic.Uint64
	written        atomic.Uint64
	errors         atomic.Uint64
	batchesDropped atomic.Uint64

	// onBatchDropped fires when a flush failed even after retry. Set by
	// the caller during construction to wire a Prometheus counter; left
	// nil-safe so unit tests don't have to.
	onBatchDropped func(rows int)

	// onSubmitDropped fires whenever Submit had to drop a row under
	// queue saturation — either the evicted oldest (on the drop-oldest
	// path) or the incoming new row (when even after eviction the
	// channel still couldn't accept). Mirrors mqtt.Publisher.OnDropped
	// semantics so callers can wire metrics + rate-limited logs.
	// Nil-safe.
	onSubmitDropped func(Observation)

	// testInserter swaps the Postgres insert path for a fake during
	// unit tests. nil in prod; never set outside writer_test.go.
	testInserter interface {
		InsertObservations(ctx context.Context, obs []Observation) error
	}
}

// ObservationsWriterOptions configures the writer. Sensible defaults
// are applied when fields are zero.
type ObservationsWriterOptions struct {
	BufferSize      int
	MaxBatch        int
	FlushInterval   time.Duration
	OnBatchDropped  func(rows int)
	OnSubmitDropped func(Observation)
}

// NewObservationsWriter creates an unstarted writer. Call Run.
func NewObservationsWriter(s *Postgres, opts ObservationsWriterOptions, logger *slog.Logger) *ObservationsWriter {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 1024
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 256
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ObservationsWriter{
		store:           s,
		in:              make(chan Observation, opts.BufferSize),
		logger:          logger,
		maxBatch:        opts.MaxBatch,
		flushTick:       opts.FlushInterval,
		onBatchDropped:  opts.OnBatchDropped,
		onSubmitDropped: opts.OnSubmitDropped,
	}
}

// Submit enqueues an observation. Non-blocking: when the buffer is
// full, the oldest queued row is evicted (and counted via Dropped)
// to make room; if even after eviction the channel still cannot
// accept, the incoming row is dropped instead — both paths increment
// Stats().Dropped and fire OnSubmitDropped. Mirrors
// mqtt.Publisher.Publish so both pipelines have the same backpressure
// semantics.
func (w *ObservationsWriter) Submit(o Observation) {
	select {
	case w.in <- o:
		return
	default:
		var evicted Observation
		droppedOldest := false
		select {
		case evicted = <-w.in:
			w.dropped.Add(1)
			droppedOldest = true
		default:
		}
		select {
		case w.in <- o:
			if droppedOldest && w.onSubmitDropped != nil {
				w.onSubmitDropped(evicted)
			}
		default:
			w.dropped.Add(1)
			if w.onSubmitDropped != nil {
				w.onSubmitDropped(o)
			}
		}
	}
}

// Stats returns a snapshot of the writer's counters.
type WriterStats struct {
	Written        uint64
	Dropped        uint64
	Errors         uint64
	BatchesDropped uint64
	Queued         int
	Cap            int
}

func (w *ObservationsWriter) Stats() WriterStats {
	return WriterStats{
		Written:        w.written.Load(),
		Dropped:        w.dropped.Load(),
		Errors:         w.errors.Load(),
		BatchesDropped: w.batchesDropped.Load(),
		Queued:         len(w.in),
		Cap:            cap(w.in),
	}
}

// Run flushes batches until ctx is cancelled. Blocks; call from a goroutine.
// On shutdown the function drains anything still buffered in w.in (in
// maxBatch-sized chunks) before returning, so a SIGTERM during a burst
// doesn't silently lose observations.
func (w *ObservationsWriter) Run(ctx context.Context) {
	t := time.NewTicker(w.flushTick)
	defer t.Stop()
	batch := make([]Observation, 0, w.maxBatch)
	for {
		select {
		case <-ctx.Done():
			w.drainAndFlush(&batch)
			return
		case o := <-w.in:
			batch = append(batch, o)
			if len(batch) >= w.maxBatch {
				w.flushBatch(&batch)
			}
		case <-t.C:
			w.flushBatch(&batch)
		}
	}
}

// flushBatch persists batch and resets the slice in place. It retries
// once with a short backoff before giving up; a sustained Postgres
// outage will surface via WriterStats.BatchesDropped and the optional
// onBatchDropped hook so /metrics + admin can alert.
//
// The drop-batch log carries a sampled set of beacon IDs (first three
// distinct slugs in the batch) so an oncall can correlate "this beacon
// stopped arriving at 14:32" with a Postgres outage dropping its arrival edge.
func (w *ObservationsWriter) flushBatch(batch *[]Observation) {
	if len(*batch) == 0 {
		return
	}
	rows := len(*batch)
	if err := w.insertWithRetry(*batch); err != nil {
		w.errors.Add(uint64(rows))
		w.batchesDropped.Add(1)
		w.logger.Error("observations flush failed after retry; dropping batch",
			"rows", rows, "err", err,
			"sample_beacons", sampleBeaconKeys(*batch, 3))
		if w.onBatchDropped != nil {
			w.onBatchDropped(rows)
		}
	} else {
		w.written.Add(uint64(rows))
	}
	*batch = (*batch)[:0]
}

// sampleBeaconKeys returns up to n distinct beacon-id slugs from the
// batch so the drop log carries forensic context without dumping the
// full row set.
func sampleBeaconKeys(batch []Observation, n int) []string {
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for _, o := range batch {
		slug := o.Kind + "." + o.Key
		if _, dup := seen[slug]; dup {
			continue
		}
		seen[slug] = struct{}{}
		out = append(out, slug)
		if len(out) >= n {
			break
		}
	}
	return out
}

// insertWithRetry attempts the insert once, then retries after a short
// backoff if the first attempt fails. Each attempt gets its own 5s
// context so a stuck pool can't hold the writer indefinitely.
func (w *ObservationsWriter) insertWithRetry(batch []Observation) error {
	attempt := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if w.testInserter != nil {
			return w.testInserter.InsertObservations(ctx, batch)
		}
		return w.store.InsertObservations(ctx, batch)
	}
	if err := attempt(); err != nil {
		w.logger.Warn("observations flush failed; retrying",
			"rows", len(batch), "err", err)
		time.Sleep(100 * time.Millisecond)
		return attempt()
	}
	return nil
}

// drainAndFlush is the shutdown path: pull anything still in w.in into
// the in-progress batch and flush in chunks. The channel is not closed
// by the writer (ingest may still hold a Submit reference); we just
// take what's there non-blockingly.
func (w *ObservationsWriter) drainAndFlush(batch *[]Observation) {
	for {
		select {
		case o := <-w.in:
			*batch = append(*batch, o)
			if len(*batch) >= w.maxBatch {
				w.flushBatch(batch)
			}
		default:
			w.flushBatch(batch)
			return
		}
	}
}
