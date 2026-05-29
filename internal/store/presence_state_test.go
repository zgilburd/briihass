package store

import (
	"context"
	"sort"
	"testing"
	"time"
)

func loadSortedPresence(t *testing.T, s *Postgres, ctx context.Context) []PresenceStateRow {
	t.Helper()
	got, err := s.LoadPresenceState(ctx)
	if err != nil {
		t.Fatalf("LoadPresenceState: %v", err)
	}
	sort.Slice(got, func(i, j int) bool {
		if got[i].Kind != got[j].Kind {
			return got[i].Kind < got[j].Kind
		}
		return got[i].Key < got[j].Key
	})
	return got
}

func TestPresenceState_RoundTrip(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()

	arrival := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	in := []PresenceStateRow{
		{Kind: "ibeacon", Key: "uuid_1_1", CurrentZone: "zone_a", CurrentAP: "aa:bb:cc:dd:ee:01", LastArrival: arrival},
		{Kind: "ibeacon", Key: "uuid_2_2", CurrentZone: "", CurrentAP: "", LastArrival: time.Time{}}, // not_home, never arrived
	}
	if err := s.SavePresenceState(ctx, in); err != nil {
		t.Fatalf("SavePresenceState: %v", err)
	}

	got := loadSortedPresence(t, s, ctx)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	if got[0].CurrentZone != "zone_a" || got[0].CurrentAP != "aa:bb:cc:dd:ee:01" {
		t.Errorf("row0 zone/ap = %q/%q, want zone_a/aa:bb:cc:dd:ee:01", got[0].CurrentZone, got[0].CurrentAP)
	}
	if !got[0].LastArrival.Equal(arrival) {
		t.Errorf("row0 LastArrival = %v, want %v", got[0].LastArrival, arrival)
	}
	if got[1].CurrentZone != "" {
		t.Errorf("row1 zone = %q, want empty (not_home)", got[1].CurrentZone)
	}
	if !got[1].LastArrival.IsZero() {
		t.Errorf("row1 LastArrival = %v, want zero (NULL)", got[1].LastArrival)
	}
}

func TestPresenceState_FullReplace(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()

	if err := s.SavePresenceState(ctx, []PresenceStateRow{
		{Kind: "ibeacon", Key: "old", CurrentZone: "zone_a"},
	}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// A later flush that no longer includes "old" must drop it.
	if err := s.SavePresenceState(ctx, []PresenceStateRow{
		{Kind: "ibeacon", Key: "new", CurrentZone: "zone_b"},
	}); err != nil {
		t.Fatalf("second save: %v", err)
	}
	got := loadSortedPresence(t, s, ctx)
	if len(got) != 1 || got[0].Key != "new" {
		t.Fatalf("got %+v, want only key=new", got)
	}
}

func TestPresenceState_EmptyClears(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()

	if err := s.SavePresenceState(ctx, []PresenceStateRow{
		{Kind: "ibeacon", Key: "x", CurrentZone: "zone_a"},
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.SavePresenceState(ctx, nil); err != nil {
		t.Fatalf("save empty: %v", err)
	}
	got := loadSortedPresence(t, s, ctx)
	if len(got) != 0 {
		t.Fatalf("got %d rows, want 0 after empty save", len(got))
	}
}

func TestPresenceState_RejectsEmptyKindKey(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	if err := s.SavePresenceState(ctx, []PresenceStateRow{{Kind: "", Key: "x"}}); err == nil {
		t.Fatal("expected error for empty kind")
	}
	if err := s.SavePresenceState(ctx, []PresenceStateRow{{Kind: "ibeacon", Key: ""}}); err == nil {
		t.Fatal("expected error for empty key")
	}
}
