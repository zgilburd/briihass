package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRawPost_InsertGet(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	id, err := s.InsertRawPost(ctx, RawPost{
		Endpoint:        "/ingest",
		RemoteAddr:      "10.0.0.1",
		ContentEncoding: EncodingGzip,
		Body:            []byte{0x1f, 0x8b, 0x08, 0x00}, // gzip magic + minimal header
		BodySHA256:      "deadbeef",
	})
	if err != nil {
		t.Fatalf("InsertRawPost: %v", err)
	}
	got, err := s.GetRawPost(ctx, id)
	if err != nil {
		t.Fatalf("GetRawPost: %v", err)
	}
	if got.Endpoint != "/ingest" || got.RemoteAddr != "10.0.0.1" || string(got.Body) != string([]byte{0x1f, 0x8b, 0x08, 0x00}) {
		t.Errorf("GetRawPost: %+v", got)
	}
	if _, err := s.GetRawPost(ctx, id+999); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetRawPost missing: want ErrNotFound, got %v", err)
	}
}

// TestRawPost_PruneOlderThan covers the raw_posts half of PruneOlderThan.
// The observations side is exercised by TestObservations_PruneOlderThan;
// without this, a regression that drops the raw_posts DELETE would
// silently grow that table until disk-full — exactly the failure mode
// retention exists to prevent.
func TestRawPost_PruneOlderThan(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	// InsertRawPost uses now() in the SQL, so we backdate one row
	// directly to simulate a stale envelope.
	oldID, err := s.InsertRawPost(ctx, RawPost{
		Endpoint: "/ingest", RemoteAddr: "10.0.0.1",
		ContentEncoding: EncodingIdentity, Body: []byte("old"), BodySHA256: "deadbeef",
	})
	if err != nil {
		t.Fatalf("InsertRawPost old: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE raw_posts SET received_at = now() - interval '72 hours' WHERE id = $1`, oldID); err != nil {
		t.Fatalf("backdate old row: %v", err)
	}
	newID, err := s.InsertRawPost(ctx, RawPost{
		Endpoint: "/ingest", RemoteAddr: "10.0.0.1",
		ContentEncoding: EncodingIdentity, Body: []byte("new"), BodySHA256: "cafebabe",
	})
	if err != nil {
		t.Fatalf("InsertRawPost new: %v", err)
	}
	_, rp, err := s.PruneOlderThan(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("PruneOlderThan: %v", err)
	}
	if rp != 1 {
		t.Errorf("PruneOlderThan raw_posts: got %d want 1", rp)
	}
	if _, err := s.GetRawPost(ctx, oldID); !errors.Is(err, ErrNotFound) {
		t.Errorf("old row should be pruned; got err=%v", err)
	}
	if _, err := s.GetRawPost(ctx, newID); err != nil {
		t.Errorf("recent row should survive prune; got err=%v", err)
	}
}
