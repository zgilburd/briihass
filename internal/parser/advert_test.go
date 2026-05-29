package parser

import (
	"strings"
	"testing"
)

func TestParseAdvert_iBeacon(t *testing.T) {
	cases := []struct {
		name     string
		hex      string
		wantUUID string
		wantMaj  uint16
		wantMin  uint16
		wantTX   int8
	}{
		{
			// Real iBeacon from a sanitized capture: well-known SDK UUID.
			//   02 01 06                                 Flags (skip)
			//   1A FF 4C 00 02 15                        Apple iBeacon manuf-data
			//   FDA50693-A4E2-4FB1-AFCF-C6EB07647825     UUID
			//   0x2751 = 10065                           Major
			//   0x65C1 = 26049                           Minor
			//   0xFD = -3                                TX
			name:     "real-ibeacon-FDA50693",
			hex:      "0201061AFF4C000215FDA50693A4E24FB1AFCFC6EB07647825275165C1FD",
			wantUUID: "fda50693-a4e2-4fb1-afcf-c6eb07647825",
			wantMaj:  10065,
			wantMin:  26049,
			wantTX:   -3,
		},
		{
			// Second real iBeacon from captures.
			//   UUID  = AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAA1
			//   Major = 0xEC05 = 60421
			//   Minor = 0x2241 = 8769
			//   TX    = 0xC5 = -59
			name:     "real-ibeacon-AAAAAAA1",
			hex:      "0201061AFF4C000215AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1EC052241C5",
			wantUUID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
			wantMaj:  60421,
			wantMin:  8769,
			wantTX:   -59,
		},
		{
			// Lowercase input -> same output (hex.DecodeString is
			// case-insensitive and our formatter always emits lowercase).
			name:     "lowercase-input",
			hex:      "0201061aff4c000215fda50693a4e24fb1afcfc6eb07647825275165c1fd",
			wantUUID: "fda50693-a4e2-4fb1-afcf-c6eb07647825",
			wantMaj:  10065,
			wantMin:  26049,
			wantTX:   -3,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ib, ok, err := ParseAdvert(tc.hex)
			if err != nil {
				t.Fatalf("ParseAdvert: %v", err)
			}
			if !ok {
				t.Fatalf("expected iBeacon, got ok=false")
			}
			if ib.UUID != tc.wantUUID {
				t.Errorf("UUID: want %q, got %q", tc.wantUUID, ib.UUID)
			}
			if ib.Major != tc.wantMaj {
				t.Errorf("Major: want %d, got %d", tc.wantMaj, ib.Major)
			}
			if ib.Minor != tc.wantMin {
				t.Errorf("Minor: want %d, got %d", tc.wantMin, ib.Minor)
			}
			if ib.TxPower != tc.wantTX {
				t.Errorf("TxPower: want %d, got %d", tc.wantTX, ib.TxPower)
			}
		})
	}
}

func TestParseAdvert_NotIBeacon(t *testing.T) {
	// Real non-iBeacon adverts seen in captures. All should parse cleanly
	// (no error) but return ok=false.
	cases := []struct {
		name string
		hex  string
	}{
		{
			// Apple Continuity (manuf-data, Apple, type byte 0x0F):
			//   02 01 1A 0E FF 4C 00 0F 05 90 00 88 D0 5C 10 02 29 04 02 0A 00
			name: "apple-continuity",
			hex:  "02011A0EFF4C000F05900088D05C10022904020A00",
		},
		{
			// Apple Nearby (type 0x10):
			//   02 01 1A 0B FF 4C 00 10 06 07 1D 94 FF B3 58 02 0A 00
			name: "apple-nearby",
			hex:  "02011A0BFF4C001006071D94FFB358020A00",
		},
		{
			// Apple Handoff (type 0x0C, "hash" subtype 0x0E):
			name: "apple-handoff",
			hex:  "02011A1BFF4C000C0E00BC7EA9F71EE1212F4A2D9FE1811006401D1E0FA918",
		},
		{
			// Apple AirDrop signal (type 0x09):
			name: "apple-airdrop",
			hex:  "02011A17FF4C00090813D30A13578C1B5816080017EF5695B0F38A",
		},
		{
			// Apple Find My (type 0x07):
			name: "apple-findmy",
			hex:  "02011A0CFF4C001007341F67AA922018020A00",
		},
		{
			// Manuf-data section with a non-Apple company ID.
			//   02 01 06 05 FF FF FF DE AD
			name: "non-apple-manuf",
			hex:  "02010605FFFFFFDEAD",
		},
		{
			// Flags only (no manuf data).
			name: "flags-only",
			hex:  "020106",
		},
		{
			// Empty.
			name: "empty",
			hex:  "",
		},
		{
			// All zero bytes — first length byte is 0, terminates immediately.
			name: "zero-terminator",
			hex:  "0000000000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ib, ok, err := ParseAdvert(tc.hex)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if ok {
				t.Fatalf("expected ok=false, got iBeacon %+v", ib)
			}
		})
	}
}

func TestParseAdvert_Errors(t *testing.T) {
	cases := []struct {
		name    string
		hex     string
		wantSub string
	}{
		{
			name:    "odd hex length",
			hex:     "020", // 3 chars, can't pair
			wantSub: "hex decode",
		},
		{
			name:    "non-hex characters",
			hex:     "ZZ",
			wantSub: "hex decode",
		},
		{
			name: "length runs past end",
			// 0xFF means "the next 255 bytes are this TLV's value" but we
			// only have 2 more bytes after the type byte.
			hex:     "FFFF0102",
			wantSub: "runs past end",
		},
		{
			name: "iBeacon signature with truncated payload",
			// 1A FF 4C 00 02 15 + only 4 bytes of UUID (16 needed). The
			// TLV length says 0x1A=26 bytes follow but we provide only
			// 6, so the bounds check fires.
			hex:     "1AFF4C00021501020304",
			wantSub: "runs past end",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ParseAdvert(tc.hex)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestParseAdvert_MultipleSections(t *testing.T) {
	// Construct an advert with: Flags + non-Apple manuf-data + Apple
	// iBeacon. The walker must skip the first two and find the iBeacon.
	//
	//   02 01 06                                     Flags
	//   05 FF FF FF DE AD                            Other-vendor manuf
	//   1A FF 4C 00 02 15 ...                        iBeacon
	hex := "020106" + "05FFFFFFDEAD" + "1AFF4C000215" +
		"FDA50693A4E24FB1AFCFC6EB07647825" +
		"2751" + "65C1" + "FD"
	ib, ok, err := ParseAdvert(hex)
	if err != nil {
		t.Fatalf("ParseAdvert: %v", err)
	}
	if !ok {
		t.Fatalf("expected iBeacon, got ok=false")
	}
	if ib.UUID != "fda50693-a4e2-4fb1-afcf-c6eb07647825" {
		t.Errorf("UUID: got %q", ib.UUID)
	}
}

func TestFormatUUID_WrongLength(t *testing.T) {
	// Unreachable from ParseAdvert (caller bounds-checks), but the
	// helper should fail closed.
	if got := formatUUID(make([]byte, 8)); got != "" {
		t.Errorf("want empty for wrong-length input, got %q", got)
	}
}
