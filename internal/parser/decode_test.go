package parser

import (
	"testing"

	"briihass/internal/ids"
)

// Synthetic advert vectors (no real beacon UUIDs — those are PII; see the
// publishable-code memo). Generated to exercise every decode + identity path.
const (
	hexIBeacon   = "0201061AFF4C0002150123456789ABCDEF0123456789ABCDEF00010002C5"
	hexEddyUID   = "0201061516AAFE00EE00112233445566778899AABBCCDDEEFF"
	hexEddyURL   = "0201060E16AAFE10EE036578616D706C6507"
	hexEddyTLM   = "0201061116AAFE20000BB8190000000064000003E8"
	hexNamed     = "0201060A0948422D544553543031020A03"
	hexAppleCont = "02010609FF4C001005AABBCCDD"
	hexIBAndName = "0201060B0953686F756C644C6F73651AFF4C0002150123456789ABCDEF0123456789ABCDEF00010002C5"
)

func TestParseIBeacon(t *testing.T) {
	a, err := Parse(hexIBeacon)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a.IBeacon == nil {
		t.Fatal("expected IBeacon frame")
	}
	if got, want := a.IBeacon.UUID, "01234567-89ab-cdef-0123-456789abcdef"; got != want {
		t.Errorf("uuid = %q, want %q", got, want)
	}
	if a.IBeacon.Major != 1 || a.IBeacon.Minor != 2 {
		t.Errorf("major/minor = %d/%d, want 1/2", a.IBeacon.Major, a.IBeacon.Minor)
	}
	if a.IBeacon.TxPower != -59 {
		t.Errorf("tx = %d, want -59", a.IBeacon.TxPower)
	}
	if a.Flags == nil || *a.Flags != 0x06 {
		t.Errorf("flags = %v, want 0x06", a.Flags)
	}
	k, ok := Identify(a)
	if !ok || k.Kind() != ids.KindIBeacon {
		t.Fatalf("Identify = %v,%v want ibeacon,true", k, ok)
	}
	if got, want := k.EntityID(), "briihass_ibeacon_0123456789abcdef0123456789abcdef_1_2"; got != want {
		t.Errorf("EntityID = %q, want %q", got, want)
	}
}

func TestParseEddystoneUID(t *testing.T) {
	a, err := Parse(hexEddyUID)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a.Eddystone == nil || a.Eddystone.FrameType != 0x00 {
		t.Fatalf("expected Eddystone UID frame, got %+v", a.Eddystone)
	}
	if a.Eddystone.Namespace != "00112233445566778899" || a.Eddystone.Instance != "aabbccddeeff" {
		t.Errorf("ns/inst = %q/%q", a.Eddystone.Namespace, a.Eddystone.Instance)
	}
	k, ok := Identify(a)
	if !ok || k.Kind() != ids.KindEddystoneUID {
		t.Fatalf("Identify = %v,%v want eddystone_uid,true", k, ok)
	}
	if got, want := k.Key(), "00112233445566778899_aabbccddeeff"; got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}

func TestParseEddystoneURL(t *testing.T) {
	a, err := Parse(hexEddyURL)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a.Eddystone == nil || a.Eddystone.URL != "https://example.com" {
		t.Fatalf("url = %+v, want https://example.com", a.Eddystone)
	}
	k, ok := Identify(a)
	if !ok || k.Kind() != ids.KindEddystoneURL {
		t.Fatalf("Identify = %v,%v want eddystone_url,true", k, ok)
	}
}

func TestParseEddystoneTLMIsAnonymousTelemetry(t *testing.T) {
	a, err := Parse(hexEddyTLM)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a.Eddystone == nil || a.Eddystone.FrameType != 0x20 {
		t.Fatalf("expected TLM frame, got %+v", a.Eddystone)
	}
	if a.Eddystone.BatteryMV == nil || *a.Eddystone.BatteryMV != 3000 {
		t.Errorf("battery = %v, want 3000", a.Eddystone.BatteryMV)
	}
	if a.Eddystone.TemperatureC == nil || *a.Eddystone.TemperatureC != 25.0 {
		t.Errorf("temp = %v, want 25.0", a.Eddystone.TemperatureC)
	}
	if a.Eddystone.AdvCount == nil || *a.Eddystone.AdvCount != 100 {
		t.Errorf("advcnt = %v, want 100", a.Eddystone.AdvCount)
	}
	if a.Eddystone.SecCount == nil || *a.Eddystone.SecCount != 1000 {
		t.Errorf("seccnt = %v, want 1000", a.Eddystone.SecCount)
	}
	if _, ok := Identify(a); ok {
		t.Error("TLM must be anonymous (no stable identity)")
	}
}

// TLM with temperature 0x8000 (the Eddystone "not available" sentinel):
// battery is decoded, temperature is left nil rather than -128.0 °C.
func TestParseEddystoneTLMTempSentinel(t *testing.T) {
	// flags + svc-data FEAA TLM: ver=00 batt=0BF4 temp=8000 adv=.. sec=..
	const hexTLMSentinel = "0201061116AAFE2000 0BF4 8000 0000000A 00000014"
	clean := ""
	for _, r := range hexTLMSentinel {
		if r != ' ' {
			clean += string(r)
		}
	}
	a, err := Parse(clean)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a.Eddystone == nil || a.Eddystone.FrameType != 0x20 {
		t.Fatalf("expected TLM frame, got %+v", a.Eddystone)
	}
	if a.Eddystone.BatteryMV == nil || *a.Eddystone.BatteryMV != 0x0BF4 {
		t.Errorf("battery = %v, want %d", a.Eddystone.BatteryMV, 0x0BF4)
	}
	if a.Eddystone.TemperatureC != nil {
		t.Errorf("temperature = %v, want nil (0x8000 sentinel)", *a.Eddystone.TemperatureC)
	}
}

func TestParseNamedDevice(t *testing.T) {
	a, err := Parse(hexNamed)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a.LocalName != "HB-TEST01" {
		t.Errorf("name = %q, want HB-TEST01", a.LocalName)
	}
	if a.TxPower == nil || *a.TxPower != 3 {
		t.Errorf("txpower = %v, want 3", a.TxPower)
	}
	k, ok := Identify(a)
	if !ok || k.Kind() != ids.KindName || k.Key() != "HB-TEST01" {
		t.Fatalf("Identify = %v,%v want name/HB-TEST01,true", k, ok)
	}
}

func TestParseAppleContinuityIsAnonymous(t *testing.T) {
	a, err := Parse(hexAppleCont)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(a.Mfg) != 1 || a.Mfg[0].CompanyID != 0x004C {
		t.Fatalf("mfg = %+v, want one Apple record", a.Mfg)
	}
	if a.IBeacon != nil {
		t.Error("Apple Continuity is not an iBeacon")
	}
	if _, ok := Identify(a); ok {
		t.Error("Apple Continuity (non-iBeacon) must be anonymous")
	}
}

func TestIdentifyPrecedenceIBeaconBeatsName(t *testing.T) {
	a, err := Parse(hexIBAndName)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a.LocalName != "ShouldLose" {
		t.Fatalf("expected name present, got %q", a.LocalName)
	}
	k, ok := Identify(a)
	if !ok || k.Kind() != ids.KindIBeacon {
		t.Fatalf("Identify = %v,%v want ibeacon (precedence over name)", k, ok)
	}
}

func TestParseErrors(t *testing.T) {
	if _, err := Parse("ZZ"); err == nil {
		t.Error("bad hex should error")
	}
	if _, err := Parse("0399"); err == nil {
		t.Error("TLV length overrun should error")
	}
	a, err := Parse("")
	if err != nil || len(a.Structures) != 0 {
		t.Errorf("empty = %+v,%v want empty,nil", a, err)
	}
}
