package ids

import (
	"errors"
	"strings"
	"testing"
)

func TestNewIBeaconKey(t *testing.T) {
	k, err := NewIBeaconKey("01234567-89AB-CDEF-0123-456789ABCDEF", 1, 2)
	if err != nil {
		t.Fatalf("NewIBeaconKey: %v", err)
	}
	if k.Kind() != KindIBeacon {
		t.Errorf("kind = %q", k.Kind())
	}
	if k.Key() != "01234567-89ab-cdef-0123-456789abcdef_1_2" {
		t.Errorf("key = %q", k.Key())
	}
	if got, want := k.EntityID(), "briihass_ibeacon_0123456789abcdef0123456789abcdef_1_2"; got != want {
		t.Errorf("EntityID = %q, want %q", got, want)
	}
	if k.Slug() != "ibeacon.01234567-89ab-cdef-0123-456789abcdef_1_2" {
		t.Errorf("slug = %q", k.Slug())
	}
	if _, err := NewIBeaconKey("not-a-uuid", 0, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("bad uuid err = %v, want ErrInvalid", err)
	}
}

func TestNewEddystoneUIDKey(t *testing.T) {
	k, err := NewEddystoneUIDKey("00112233445566778899", "AABBCCDDEEFF")
	if err != nil {
		t.Fatalf("NewEddystoneUIDKey: %v", err)
	}
	if k.Key() != "00112233445566778899_aabbccddeeff" {
		t.Errorf("key = %q", k.Key())
	}
	if got := k.EntityID(); got != "briihass_eddystone_uid_00112233445566778899_aabbccddeeff" {
		t.Errorf("EntityID = %q", got)
	}
	for _, bad := range [][2]string{{"short", "aabbccddeeff"}, {"00112233445566778899", "xyz"}} {
		if _, err := NewEddystoneUIDKey(bad[0], bad[1]); !errors.Is(err, ErrInvalid) {
			t.Errorf("NewEddystoneUIDKey(%v) err = %v, want ErrInvalid", bad, err)
		}
	}
}

func TestNewNameKeyAndEntityIDSanitization(t *testing.T) {
	k, err := NewNameKey("HB-TEST 01")
	if err != nil {
		t.Fatalf("NewNameKey: %v", err)
	}
	if k.Key() != "HB-TEST 01" {
		t.Errorf("key = %q (should preserve original)", k.Key())
	}
	id := k.EntityID()
	if !strings.HasPrefix(id, "briihass_name_hb_test_01_") {
		t.Errorf("EntityID = %q, want sanitized readable prefix", id)
	}
	if _, err := NewNameKey("   "); !errors.Is(err, ErrInvalid) {
		t.Errorf("empty name err = %v, want ErrInvalid", err)
	}
}

// Two free-form keys that sanitize to the same readable form must still
// produce distinct entity ids (the hash suffix guarantees injectivity).
func TestEntityIDFreeFormCollisionGuard(t *testing.T) {
	a, _ := NewNameKey("a-b")
	b, _ := NewNameKey("a b")
	if a.EntityID() == b.EntityID() {
		t.Errorf("distinct keys collided: %q", a.EntityID())
	}
	if !strings.HasPrefix(a.EntityID(), "briihass_name_a_b_") {
		t.Errorf("unexpected: %q", a.EntityID())
	}
}

func TestNewURLAndMfgKeys(t *testing.T) {
	u, err := NewEddystoneURLKey("HTTPS://Example.com")
	if err != nil || u.Key() != "https://example.com" {
		t.Fatalf("url key = %q, %v", u.Key(), err)
	}
	if _, err := NewEddystoneURLKey(""); !errors.Is(err, ErrInvalid) {
		t.Errorf("empty url err = %v", err)
	}
	m, err := NewMfgKey(0x004C, []byte{1, 2, 3})
	if err != nil || !strings.HasPrefix(m.Key(), "004c_") {
		t.Fatalf("mfg key = %q, %v", m.Key(), err)
	}
	// Stable: same input → same key.
	m2, _ := NewMfgKey(0x004C, []byte{1, 2, 3})
	if m.Key() != m2.Key() {
		t.Error("mfg key not deterministic")
	}
	if _, err := NewMfgKey(0x004C, nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("empty payload err = %v", err)
	}
}

func TestSlugRoundTrip(t *testing.T) {
	orig, _ := NewIBeaconKey("01234567-89ab-cdef-0123-456789abcdef", 7, 9)
	got, ok := ParseSlug(orig.Slug())
	if !ok {
		t.Fatal("ParseSlug failed")
	}
	if got.Kind() != orig.Kind() || got.Key() != orig.Key() {
		t.Errorf("round-trip = %v, want %v", got, orig)
	}
	for _, bad := range []string{"", "nodot", "bogus.key", "ibeacon."} {
		if _, ok := ParseSlug(bad); ok {
			t.Errorf("ParseSlug(%q) should fail", bad)
		}
	}
}

func TestFromStoreKey(t *testing.T) {
	k, ok := FromStoreKey("name", "Widget")
	if !ok || k.Kind() != KindName || k.Key() != "Widget" {
		t.Errorf("FromStoreKey = %v,%v", k, ok)
	}
	if _, ok := FromStoreKey("bogus", "x"); ok {
		t.Error("unknown kind should fail")
	}
	if _, ok := FromStoreKey("name", ""); ok {
		t.Error("empty key should fail")
	}
}

func TestIsZero(t *testing.T) {
	var z BeaconKey
	if !z.IsZero() {
		t.Error("zero value should be IsZero")
	}
	k, _ := NewNameKey("x")
	if k.IsZero() {
		t.Error("constructed key should not be IsZero")
	}
}
