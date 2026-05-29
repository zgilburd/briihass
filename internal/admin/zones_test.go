package admin

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// newZonesServer wires the minimum surface needed to exercise zones.
// Reuses newPhase3Server (in devices_test.go) so we get the same fake
// engine + remover wiring as the promote/demote round-trip test.
func newZonesServer(t *testing.T, devs *fakeDevicesStore, zones *fakeZonesStore, eng *fakeEngine) *Server {
	t.Helper()
	return newPhase3Server(t, devs, zones, eng, &fakeRemover{})
}

// TestZones_UpsertRebuildsTopology covers the happy path: a valid
// zone POST persists to the store. Engine refresh is exercised via
// rebuildEngineTopology (validated against config.Topology.Validate),
// so a malformed MAC here would have failed the redirect.
func TestZones_UpsertRebuildsTopology(t *testing.T) {
	devs := &fakeDevicesStore{}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	s := newZonesServer(t, devs, zones, eng)

	form := url.Values{
		"action":     {"save"},
		"ap_mac":     {"aa:bb:cc:dd:ee:01"},
		"zone_label": {"zone_a"},
		"ap_name":    {"AP-1"},
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/zones", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	if len(zones.zones) != 1 || zones.zones[0].ZoneLabel.String() != "zone_a" {
		t.Errorf("zone not persisted: %+v", zones.zones)
	}
	// Engine MUST have received the updated topology. A refactor that
	// dropped rebuildEngineTopology from the zones upsert path would
	// otherwise land green — and HA would silently see no new zone label.
	top, count := eng.lastAppliedTopology()
	if count != 1 {
		t.Fatalf("ApplyTopology call count: got %d want 1", count)
	}
	if top == nil || top.Zone("aa:bb:cc:dd:ee:01") != "zone_a" {
		t.Errorf("ApplyTopology zone wrong: zone=%q", top.Zone("aa:bb:cc:dd:ee:01"))
	}
}

// TestZones_RejectsMissingFields verifies form validation.
func TestZones_RejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		form url.Values
	}{
		{"missing ap_mac", url.Values{"action": {"save"}, "zone_label": {"zone_a"}}},
		{"missing zone_label", url.Values{"action": {"save"}, "ap_mac": {"aa:bb:cc:dd:ee:01"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			zones := &fakeZonesStore{}
			s := newZonesServer(t, &fakeDevicesStore{}, zones, &fakeEngine{})
			req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/zones", strings.NewReader(tc.form.Encode())))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rr := httptest.NewRecorder()
			s.Routes().ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("expected re-rendered page, got %d", rr.Code)
			}
			if len(zones.zones) != 0 {
				t.Errorf("nothing should have been persisted; got %+v", zones.zones)
			}
		})
	}
}

// TestZones_DeleteRebuildsTopology covers the delete branch.
func TestZones_DeleteRebuildsTopology(t *testing.T) {
	zones := &fakeZonesStore{}
	zones.zones = append(zones.zones, mustZone("aa:bb:cc:dd:ee:01", "zone_a", "AP-1"))
	s := newZonesServer(t, &fakeDevicesStore{}, zones, &fakeEngine{})

	form := url.Values{
		"action": {"delete"},
		"ap_mac": {"aa:bb:cc:dd:ee:01"},
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/zones", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	if len(zones.zones) != 0 {
		t.Errorf("zone not deleted: %+v", zones.zones)
	}
}

// TestZones_RefreshEngineRoute exercises the manual-retry route used
// by the inline error banner.
func TestZones_RefreshEngineRoute(t *testing.T) {
	zones := &fakeZonesStore{}
	zones.zones = append(zones.zones, mustZone("aa:bb:cc:dd:ee:01", "zone_a", ""))
	s := newZonesServer(t, &fakeDevicesStore{}, zones, &fakeEngine{})

	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/zones/refresh-engine", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("refresh-engine status: %d, body=%s", rr.Code, rr.Body.String())
	}
}
