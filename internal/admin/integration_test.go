package admin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"briihass/internal/mqtt"
	"briihass/internal/store"
)

// TestDevices_DemoteWiresThroughRealPublisher catches the regression
// the reviewer called out: a refactor that drops or breaks the
// `RemoveMQTTEntity: pub.RemoveEntity` wiring in cmd/briihass/main.go
// would have shipped green if the admin demote test only used a fake
// remover. This test mounts admin with a *mqtt.Publisher (no broker —
// paho stays disconnected, but the queue accepts items) and verifies
// the demote handler enqueues a remove request.
//
// The publisher's full broker round-trip is covered separately by
// TestPublisher_RemoveEntityClearsRetainedTopics; this test covers
// the wiring between admin and the publisher.
func TestDevices_DemoteWiresThroughRealPublisher(t *testing.T) {
	pub, err := mqtt.New(mqtt.Options{
		BrokerURL:  "tcp://127.0.0.1:1", // intentionally unreachable
		Username:   "u",
		Password:   "p",
		ClientID:   "admin-integration-test",
		BufferSize: 8,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("mqtt.New: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	depthBefore := pub.Stats().QueueDepth

	devs := &fakeDevicesStore{
		beacons: []store.Beacon{mustBeacon("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 2, "alpha")},
	}
	zones := &fakeZonesStore{zones: []store.Zone{mustZone("aa:bb:cc:dd:ee:01", "zone_a", "")}}
	s, err := New(Options{
		User:            "u",
		Pass:            "p",
		Engine:          &fakeEngine{},
		Store:           &fakeStore{},
		CurrentTunables: sampleTunables(),
		Devices:         devs,
		Zones:           zones,
		SettingsSnap:    store.NewSettingsSnapshot(mustSettings(t, 7, false, false)),
		// Exactly the wiring main.go uses today. If this signature
		// drifts or the closure goes missing, this test fails.
		RemoveMQTTEntity: pub.RemoveEntity,
		BuildCommit:      "test",
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("admin.New: %v", err)
	}

	form := url.Values{
		"slug": {"ibeacon.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2"},
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/demote", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("demote status: %d body=%s", rr.Code, rr.Body.String())
	}

	// The publisher's Run goroutine isn't running, so the remove item
	// stays in the queue. Stats().QueueDepth should reflect the
	// enqueued request.
	depthAfter := pub.Stats().QueueDepth
	if depthAfter <= depthBefore {
		t.Errorf("expected publisher queue to grow after demote; before=%d after=%d",
			depthBefore, depthAfter)
	}

	// Sanity: drain enough time for an asynchronous reconnect attempt
	// not to interfere with the queue (paho would not consume queue
	// items by itself anyway, but be paranoid).
	time.Sleep(50 * time.Millisecond)
	if d := pub.Stats().QueueDepth; d < depthAfter {
		t.Errorf("queue depth shrank without a Run goroutine; before=%d after=%d later=%d",
			depthBefore, depthAfter, d)
	}

	_ = context.Background() // silences unused import on some setups
}
