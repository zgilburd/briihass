package ids

import (
	"errors"
	"strings"
	"testing"
)

func TestNewAPMAC_Valid(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"aa:bb:cc:dd:ee:ff", "aa:bb:cc:dd:ee:ff"},
		{"AA:BB:CC:DD:EE:FF", "aa:bb:cc:dd:ee:ff"},
		{"aa-bb-cc-dd-ee-ff", "aa:bb:cc:dd:ee:ff"},
		{"  aa:bb:cc:dd:ee:ff  ", "aa:bb:cc:dd:ee:ff"},
		{"00:00:00:00:00:00", "00:00:00:00:00:00"},
	}
	for _, tc := range cases {
		got, err := NewAPMAC(tc.in)
		if err != nil {
			t.Errorf("NewAPMAC(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got.String() != tc.want {
			t.Errorf("NewAPMAC(%q).String() = %q, want %q", tc.in, got.String(), tc.want)
		}
		if got.IsZero() {
			t.Errorf("NewAPMAC(%q).IsZero() = true, want false", tc.in)
		}
	}
}

func TestNewAPMAC_Invalid(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"not_a_mac",
		"aa:bb:cc:dd:ee",          // 5 bytes
		"aa:bb:cc:dd:ee:ff:00",    // 7 bytes
		"aa:bb:cc:dd:ee:ff:00:11", // EUI-64
		"zz:zz:zz:zz:zz:zz",
		"aa.bb.cc.dd.ee.ff",
	}
	for _, in := range cases {
		_, err := NewAPMAC(in)
		if err == nil {
			t.Errorf("NewAPMAC(%q): expected error, got nil", in)
			continue
		}
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("NewAPMAC(%q): error %v does not wrap ErrInvalid", in, err)
		}
	}
}

func TestAPMAC_ZeroValue(t *testing.T) {
	var m APMAC
	if !m.IsZero() {
		t.Error("zero-value APMAC.IsZero() = false, want true")
	}
	if m.String() != "" {
		t.Errorf("zero-value APMAC.String() = %q, want empty", m.String())
	}
}

func TestMustNewAPMAC_PanicsOnInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewAPMAC(\"not_a_mac\") did not panic")
		}
	}()
	_ = MustNewAPMAC("not_a_mac")
}

func TestNewAPMAC_DashFormCanonicalizesToColon(t *testing.T) {
	m, err := NewAPMAC("AA-bb-CC-dd-EE-ff")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(m.String(), ":") {
		t.Errorf("expected colon-separated canonical form, got %q", m.String())
	}
}
