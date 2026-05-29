package parser

import "testing"

func TestParseAdvertDebug_iBeacon(t *testing.T) {
	// Same fixture as TestParseAdvert_iBeacon.
	const h = "0201061AFF4C000215FDA50693A4E24FB1AFCFC6EB07647825275165C1FD"
	walk, err := ParseAdvertDebug(h)
	if err != nil {
		t.Fatalf("ParseAdvertDebug: %v", err)
	}
	if walk.IBeacon == nil || walk.IBeacon.UUID != "fda50693-a4e2-4fb1-afcf-c6eb07647825" {
		t.Fatalf("IBeacon: %+v", walk.IBeacon)
	}
	if len(walk.Records) != 2 {
		t.Fatalf("Records: want 2, got %d", len(walk.Records))
	}
	if walk.Records[0].Type != 0x01 {
		t.Errorf("Records[0].Type: want 0x01 (Flags), got 0x%02X", walk.Records[0].Type)
	}
	if walk.Records[1].Type != 0xFF || walk.Records[1].Label != "Manufacturer-specific (Apple iBeacon)" {
		t.Errorf("Records[1]: %+v", walk.Records[1])
	}
}

func TestParseAdvertDebug_NotIBeacon(t *testing.T) {
	// AltBeacon-ish: Apple company id but iBeacon marker bytes wrong.
	const h = "0201061AFF4C00000115FDA50693A4E24FB1AFCFC6EB07647825275165C1FD"
	walk, err := ParseAdvertDebug(h)
	if err != nil {
		t.Fatalf("ParseAdvertDebug: %v", err)
	}
	if walk.IBeacon != nil {
		t.Errorf("expected no iBeacon, got %+v", walk.IBeacon)
	}
	if len(walk.Records) != 2 {
		t.Fatalf("Records: want 2, got %d", len(walk.Records))
	}
}

func TestParseAdvertDebug_MalformedTLV(t *testing.T) {
	// Length byte runs past the buffer.
	const h = "FF01"
	walk, err := ParseAdvertDebug(h)
	if err != nil {
		t.Fatalf("ParseAdvertDebug: %v", err)
	}
	if walk.Err == "" {
		t.Error("expected Err to be set for malformed TLV")
	}
}
