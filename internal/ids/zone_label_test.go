package ids

import (
	"errors"
	"strings"
	"testing"
)

func TestNewZoneLabel_Valid(t *testing.T) {
	cases := []string{
		"a",
		"zone_a",
		"entry",
		"zone_1",
		"z",
		"abcdefghijklmnop_0123456789",
	}
	for _, in := range cases {
		got, err := NewZoneLabel(in)
		if err != nil {
			t.Errorf("NewZoneLabel(%q) unexpected error: %v", in, err)
			continue
		}
		if got.String() != in {
			t.Errorf("NewZoneLabel(%q).String() = %q, want %q", in, got.String(), in)
		}
		if got.IsZero() {
			t.Errorf("NewZoneLabel(%q).IsZero() = true, want false", in)
		}
		if !IsValidZoneLabel(in) {
			t.Errorf("IsValidZoneLabel(%q) = false, want true", in)
		}
	}
}

func TestNewZoneLabel_Invalid(t *testing.T) {
	cases := []string{
		"",
		"Zone_A",                // uppercase
		"zone a",                // space
		"1zone",                 // leading digit
		"_zone",                 // leading underscore
		"entry!",                // punctuation
		"zone-a",                // dash
		strings.Repeat("a", 65), // too long
	}
	for _, in := range cases {
		_, err := NewZoneLabel(in)
		if err == nil {
			t.Errorf("NewZoneLabel(%q): expected error, got nil", in)
			continue
		}
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("NewZoneLabel(%q): error %v does not wrap ErrInvalid", in, err)
		}
		if IsValidZoneLabel(in) {
			t.Errorf("IsValidZoneLabel(%q) = true, want false", in)
		}
	}
}

func TestZoneLabel_ZeroValue(t *testing.T) {
	var z ZoneLabel
	if !z.IsZero() {
		t.Error("zero-value ZoneLabel.IsZero() = false, want true")
	}
	if z.String() != "" {
		t.Errorf("zero-value ZoneLabel.String() = %q, want empty", z.String())
	}
}

func TestMustNewZoneLabel_PanicsOnInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewZoneLabel(\"Zone_A\") did not panic")
		}
	}()
	_ = MustNewZoneLabel("Zone_A")
}

func TestZoneLabelFromStoreRow_Bypass(t *testing.T) {
	// FromStoreRow trusts the schema and does not re-validate.
	z := ZoneLabelFromStoreRow("not-validated")
	if z.String() != "not-validated" {
		t.Errorf("ZoneLabelFromStoreRow round-trip: got %q", z.String())
	}
}
