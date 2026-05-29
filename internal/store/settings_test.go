package store

import (
	"context"
	"testing"
)

func TestSettings_LoadSaveRoundtrip(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	// Schema seeds a default row; LoadSettings should never fail.
	cur, err := s.LoadSettings(ctx)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if cur.RetentionDays() != 7 || !cur.CapturePerEventHex() || cur.CaptureFullPosts() {
		t.Errorf("default settings: %+v", cur)
	}
	next, err := NewSettings(14, false, true)
	if err != nil {
		t.Fatalf("NewSettings: %v", err)
	}
	if err := s.SaveSettings(ctx, next); err != nil {
		t.Fatalf("SaveSettings: %v", err)
	}
	got, err := s.LoadSettings(ctx)
	if err != nil {
		t.Fatalf("LoadSettings again: %v", err)
	}
	if got != next {
		t.Errorf("LoadSettings: %+v want %+v", got, next)
	}
}

func TestSettings_SaveRejectsOutOfRange(t *testing.T) {
	s := testPostgres(t)
	// Zero-value Settings has retentionDays=0; SaveSettings calls
	// NewSettings internally and rejects it.
	if err := s.SaveSettings(context.Background(), Settings{}); err == nil {
		t.Error("want error for zero-value (retention_days=0)")
	}
	// NewSettings rejects above-bound values; the only way to even
	// build a Settings outside [1, 30] is via the constructor since
	// the fields are unexported.
	if _, err := NewSettings(0, false, false); err == nil {
		t.Error("NewSettings(0): want error")
	}
	if _, err := NewSettings(100, false, false); err == nil {
		t.Error("NewSettings(100): want error")
	}
}

func TestSettingsSnapshot(t *testing.T) {
	initial, err := NewSettings(7, true, false)
	if err != nil {
		t.Fatalf("NewSettings: %v", err)
	}
	snap := NewSettingsSnapshot(initial)
	if got := snap.Get(); got.RetentionDays() != 7 || !got.CapturePerEventHex() {
		t.Errorf("initial Get: %+v", got)
	}
	next, err := NewSettings(30, false, true)
	if err != nil {
		t.Fatalf("NewSettings: %v", err)
	}
	if err := snap.Replace(next); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if got := snap.Get(); got.RetentionDays() != 30 || !got.CaptureFullPosts() || got.CapturePerEventHex() {
		t.Errorf("after Replace: %+v", got)
	}
}
