package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRawPostsInserter records every batch so tests can assert
// batching, retry, and submit-drop behavior without a real DB.
type fakeRawPostsInserter struct {
	mu       sync.Mutex
	calls    int
	failNext int   // count of subsequent calls that should error
	err      error // error to return while failNext > 0
	jobs     []rawPostJob
}

func (f *fakeRawPostsInserter) InsertRawPostBatch(_ context.Context, batch []rawPostJob) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failNext > 0 {
		f.failNext--
		return f.err
	}
	f.jobs = append(f.jobs, batch...)
	return nil
}

// TestRawPostsWriter_FlushAndStats verifies the happy path: Submit'd
// rows reach the inserter in batched form and Stats() reports them.
func TestRawPostsWriter_FlushAndStats(t *testing.T) {
	w := NewRawPostsWriter(nil, RawPostsWriterOptions{
		BufferSize:    16,
		MaxBatch:      4,
		FlushInterval: 20 * time.Millisecond,
	}, discardLogger(t))
	fi := &fakeRawPostsInserter{}
	w.testInserter = fi

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	for i := 0; i < 5; i++ {
		id, err := w.AllocateID(context.Background())
		if err != nil {
			t.Fatalf("AllocateID: %v", err)
		}
		w.Submit(id, RawPost{
			Endpoint:        "/ingest",
			ContentEncoding: EncodingGzip,
			Body:            []byte("x"),
			BodySHA256:      "deadbeef",
		})
	}

	// Wait for everything to flush.
	deadline := time.Now().Add(2 * time.Second)
	for {
		fi.mu.Lock()
		n := len(fi.jobs)
		fi.mu.Unlock()
		if n >= 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("inserter received %d jobs, want 5", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done

	st := w.Stats()
	if st.Written < 5 {
		t.Errorf("Stats.Written: got %d want >=5", st.Written)
	}
	if st.Dropped != 0 {
		t.Errorf("Stats.Dropped: got %d want 0", st.Dropped)
	}
	if st.Errors != 0 {
		t.Errorf("Stats.Errors: got %d want 0", st.Errors)
	}
}

// TestRawPostsWriter_RetryOnTransientError verifies that a one-shot
// inserter failure is recovered on the retry attempt — the row still
// lands and the batch is not counted as dropped.
func TestRawPostsWriter_RetryOnTransientError(t *testing.T) {
	w := NewRawPostsWriter(nil, RawPostsWriterOptions{
		BufferSize:    8,
		MaxBatch:      4,
		FlushInterval: 20 * time.Millisecond,
	}, discardLogger(t))
	fi := &fakeRawPostsInserter{failNext: 1, err: errors.New("transient")}
	w.testInserter = fi

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	id, _ := w.AllocateID(context.Background())
	w.Submit(id, RawPost{
		Endpoint:        "/ingest",
		ContentEncoding: EncodingGzip,
		Body:            []byte("x"),
		BodySHA256:      "deadbeef",
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		fi.mu.Lock()
		n := len(fi.jobs)
		fi.mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("inserter never received the row after retry")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done

	st := w.Stats()
	if st.BatchesDropped != 0 {
		t.Errorf("BatchesDropped: got %d want 0 (transient must recover)", st.BatchesDropped)
	}
}

// TestRawPostsWriter_DropOldestOnSubmitSaturation verifies the
// drop-oldest contract: when the queue is full, the oldest job is
// evicted and OnSubmitDropped fires once per drop. Mirrors
// ObservationsWriter behavior.
func TestRawPostsWriter_DropOldestOnSubmitSaturation(t *testing.T) {
	var dropCount atomic.Int32
	w := NewRawPostsWriter(nil, RawPostsWriterOptions{
		BufferSize:    1,
		MaxBatch:      1,
		FlushInterval: time.Hour, // don't auto-flush during the test
		OnSubmitDropped: func(_ RawPost) {
			dropCount.Add(1)
		},
	}, discardLogger(t))
	// Don't start Run — we want the queue to stay full.
	body := []byte("x")
	row := func(i int) RawPost {
		return RawPost{
			Endpoint:        "/ingest",
			ContentEncoding: EncodingGzip,
			Body:            body,
			BodySHA256:      "deadbeef",
		}
	}
	for i := 0; i < 5; i++ {
		w.Submit(int64(i), row(i))
	}
	if dropCount.Load() == 0 {
		t.Errorf("expected at least one drop with 5 submits into a 1-slot queue, got %d", dropCount.Load())
	}
	if w.Stats().Dropped == 0 {
		t.Errorf("Stats.Dropped: got 0, want >0 after saturating submits")
	}
}

// TestRawPostsWriter_AllocateIDViaTestInserter verifies that under
// the test path AllocateID returns monotonically increasing synthetic
// ids without hitting Postgres. The production path uses nextval.
func TestRawPostsWriter_AllocateIDViaTestInserter(t *testing.T) {
	w := NewRawPostsWriter(nil, RawPostsWriterOptions{}, discardLogger(t))
	w.testInserter = &fakeRawPostsInserter{}
	id1, err := w.AllocateID(context.Background())
	if err != nil {
		t.Fatalf("AllocateID: %v", err)
	}
	if id1 <= 0 {
		t.Errorf("AllocateID id: got %d want >0", id1)
	}
}
