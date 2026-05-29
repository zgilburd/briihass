package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// RawPostsWriter drains raw_post inserts from a channel and flushes
// them in batches, mirroring the ObservationsWriter contract so the
// ingest hot path never blocks on a DB round trip.
//
// IDs are pre-allocated via SELECT nextval('raw_posts_id_seq') in the
// hot path so observations can carry an immediate RawPostID pointer.
// The actual INSERT happens later from the writer goroutine. The FK
// from observations.raw_post_id → raw_posts.id was dropped in the
// matching schema change (see schema.sql header) precisely so an
// observation can land before its corresponding envelope row — the
// pointer is best-effort by design.
type RawPostsWriter struct {
	store     *Postgres
	in        chan rawPostJob
	logger    *slog.Logger
	maxBatch  int
	flushTick time.Duration

	dropped        atomic.Uint64
	written        atomic.Uint64
	errors         atomic.Uint64
	batchesDropped atomic.Uint64

	onBatchDropped  func(rows int)
	onSubmitDropped func(RawPost)

	// testInserter swaps the Postgres insert path for a fake during
	// unit tests. nil in prod.
	testInserter interface {
		InsertRawPostBatch(ctx context.Context, jobs []rawPostJob) error
	}
}

// rawPostJob is a pre-allocated id paired with the RawPost body.
// Visible to the writer_test.go for the testInserter contract.
type rawPostJob struct {
	ID  int64
	Row RawPost
}

// RawPostsWriterOptions configures the writer. Sensible defaults are
// applied when fields are zero.
type RawPostsWriterOptions struct {
	BufferSize      int
	MaxBatch        int
	FlushInterval   time.Duration
	OnBatchDropped  func(rows int)
	OnSubmitDropped func(RawPost)
}

// NewRawPostsWriter creates an unstarted writer. Call Run.
func NewRawPostsWriter(s *Postgres, opts RawPostsWriterOptions, logger *slog.Logger) *RawPostsWriter {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 256
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 32
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &RawPostsWriter{
		store:           s,
		in:              make(chan rawPostJob, opts.BufferSize),
		logger:          logger,
		maxBatch:        opts.MaxBatch,
		flushTick:       opts.FlushInterval,
		onBatchDropped:  opts.OnBatchDropped,
		onSubmitDropped: opts.OnSubmitDropped,
	}
}

// AllocateID returns a freshly reserved raw_posts.id without waiting
// for the row to be INSERTed. Sub-millisecond synchronous call —
// nextval is not subject to write contention.
//
// Used by ingest to stamp observations with a RawPostID before
// handing the envelope to Submit. If AllocateID fails the caller
// should treat capture as off for the request (no RawPostID on
// observations, no Submit call).
func (w *RawPostsWriter) AllocateID(ctx context.Context) (int64, error) {
	if w.testInserter != nil {
		// Tests opt out of the real sequence — emit a synthetic id
		// based on the write counter so each call is unique.
		return int64(w.written.Load()+w.dropped.Load()) + 1, nil
	}
	var id int64
	if err := w.store.pool.QueryRow(ctx, `SELECT nextval('raw_posts_id_seq')`).Scan(&id); err != nil {
		return 0, fmt.Errorf("alloc raw_post id: %w", err)
	}
	return id, nil
}

// Submit enqueues a raw_post for asynchronous insertion. Non-blocking:
// when the buffer is full, the oldest queued row is evicted (and
// counted via Dropped) to make room. Mirrors ObservationsWriter and
// mqtt.Publisher backpressure semantics.
func (w *RawPostsWriter) Submit(id int64, row RawPost) {
	job := rawPostJob{ID: id, Row: row}
	select {
	case w.in <- job:
		return
	default:
		var evicted rawPostJob
		droppedOldest := false
		select {
		case evicted = <-w.in:
			w.dropped.Add(1)
			droppedOldest = true
		default:
		}
		select {
		case w.in <- job:
			if droppedOldest && w.onSubmitDropped != nil {
				w.onSubmitDropped(evicted.Row)
			}
		default:
			w.dropped.Add(1)
			if w.onSubmitDropped != nil {
				w.onSubmitDropped(job.Row)
			}
		}
	}
}

// Stats returns a snapshot of the writer's counters. Reuses
// WriterStats (defined in writer.go) so admin/status can present
// the two writers uniformly.
func (w *RawPostsWriter) Stats() WriterStats {
	return WriterStats{
		Written:        w.written.Load(),
		Dropped:        w.dropped.Load(),
		Errors:         w.errors.Load(),
		BatchesDropped: w.batchesDropped.Load(),
		Queued:         len(w.in),
		Cap:            cap(w.in),
	}
}

// Run flushes batches until ctx is cancelled. Blocks; call from a
// goroutine. On shutdown, anything still buffered is drained and
// flushed before returning.
func (w *RawPostsWriter) Run(ctx context.Context) {
	t := time.NewTicker(w.flushTick)
	defer t.Stop()
	batch := make([]rawPostJob, 0, w.maxBatch)
	for {
		select {
		case <-ctx.Done():
			w.drainAndFlush(&batch)
			return
		case job := <-w.in:
			batch = append(batch, job)
			if len(batch) >= w.maxBatch {
				w.flushBatch(&batch)
			}
		case <-t.C:
			w.flushBatch(&batch)
		}
	}
}

func (w *RawPostsWriter) flushBatch(batch *[]rawPostJob) {
	if len(*batch) == 0 {
		return
	}
	rows := len(*batch)
	if err := w.insertWithRetry(*batch); err != nil {
		w.errors.Add(uint64(rows))
		w.batchesDropped.Add(1)
		w.logger.Error("raw_posts flush failed after retry; dropping batch",
			"rows", rows, "err", err)
		if w.onBatchDropped != nil {
			w.onBatchDropped(rows)
		}
	} else {
		w.written.Add(uint64(rows))
	}
	*batch = (*batch)[:0]
}

func (w *RawPostsWriter) insertWithRetry(batch []rawPostJob) error {
	attempt := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if w.testInserter != nil {
			return w.testInserter.InsertRawPostBatch(ctx, batch)
		}
		return w.store.insertRawPostBatch(ctx, batch)
	}
	if err := attempt(); err != nil {
		w.logger.Warn("raw_posts flush failed; retrying",
			"rows", len(batch), "err", err)
		time.Sleep(100 * time.Millisecond)
		return attempt()
	}
	return nil
}

func (w *RawPostsWriter) drainAndFlush(batch *[]rawPostJob) {
	for {
		select {
		case job := <-w.in:
			*batch = append(*batch, job)
			if len(*batch) >= w.maxBatch {
				w.flushBatch(batch)
			}
		default:
			w.flushBatch(batch)
			return
		}
	}
}

// insertRawPostBatch writes a batch via pgx.CopyFrom. Each row carries
// the id pre-allocated by AllocateID so observations referencing these
// envelopes can persist in any order (the FK on observations.raw_post_id
// was dropped; see schema.sql).
func (s *Postgres) insertRawPostBatch(ctx context.Context, batch []rawPostJob) error {
	if len(batch) == 0 {
		return nil
	}
	// Validate the batch up front so a single bad row can't partially
	// commit. Same shape as InsertObservations.
	for i, j := range batch {
		if j.Row.Endpoint == "" {
			return fmt.Errorf("rawPostJob[%d]: endpoint required", i)
		}
		if len(j.Row.Body) == 0 {
			return fmt.Errorf("rawPostJob[%d]: body required", i)
		}
		if j.Row.BodySHA256 == "" {
			return fmt.Errorf("rawPostJob[%d]: body_sha256 required", i)
		}
		switch j.Row.ContentEncoding {
		case EncodingGzip, EncodingIdentity:
		case "":
			return fmt.Errorf("rawPostJob[%d]: ContentEncoding required", i)
		default:
			return fmt.Errorf("rawPostJob[%d]: unknown ContentEncoding %q", i, j.Row.ContentEncoding)
		}
	}
	rows := make([][]any, len(batch))
	for i, j := range batch {
		var recv any
		if !j.Row.ReceivedAt.IsZero() {
			recv = j.Row.ReceivedAt
		} else {
			recv = time.Now()
		}
		var remote, ce any
		if j.Row.RemoteAddr != "" {
			remote = j.Row.RemoteAddr
		}
		if j.Row.ContentEncoding != "" {
			ce = string(j.Row.ContentEncoding)
		}
		rows[i] = []any{j.ID, recv, j.Row.Endpoint, remote, ce, j.Row.Body, j.Row.BodySHA256}
	}
	cols := []string{"id", "received_at", "endpoint", "remote_addr", "content_encoding", "body_gzip", "body_sha256"}
	n, err := s.pool.CopyFrom(ctx, pgx.Identifier{"raw_posts"}, cols, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("copy raw_posts: %w", err)
	}
	if int(n) != len(rows) {
		return fmt.Errorf("copy raw_posts: wrote %d of %d rows", n, len(rows))
	}
	return nil
}

// ErrRawPostAlloc is returned (wrapped) when AllocateID fails. Callers
// should treat capture as off for the request rather than blocking on
// the failure.
var ErrRawPostAlloc = errors.New("raw post id allocation failed")
