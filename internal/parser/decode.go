// decode.go is the BLE Scan plugin parser: it fully walks a BLE
// advertisement TLV and decodes every AD structure, then a separate
// classifier (Identify) derives a packet-derived ids.BeaconKey by
// precedence. Unlike the legacy ParseAdvert (iBeacon-only filter in
// advert.go), Parse never drops non-iBeacon adverts — classification is
// a downstream decision so observations/metrics can see everything.
//
// See ADR-0008 for the identity model.
package parser

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"briihass/internal/ids"
)

// ADStructure is one decoded (type, value) record from the advert.
type ADStructure struct {
	Type  byte
	Value []byte
}

// MfgRecord is a manufacturer-specific (0xFF) AD structure split into
// its little-endian company id and the trailing payload.
type MfgRecord struct {
	CompanyID uint16
	Payload   []byte
}

// SvcDataRecord is a service-data AD structure (0x16 / 0x20 / 0x21).
type SvcDataRecord struct {
	Is128   bool
	UUID16  uint16 // valid when !Is128
	UUID128 string // hex, valid when Is128
	Payload []byte // bytes after the service UUID
}

// IBeaconFrame is a decoded Apple iBeacon. Mirrors the legacy IBeacon
// struct in advert.go but lives on the richer Advert.
type IBeaconFrame struct {
	UUID    string // canonical lowercase 8-4-4-4-12
	Major   uint16
	Minor   uint16
	TxPower int8
}

// EddystoneFrame is a decoded Eddystone service-data frame. Only the
// fields relevant to the present FrameType are populated; pointers are
// nil when the field is absent.
type EddystoneFrame struct {
	FrameType    byte // 0x00 UID | 0x10 URL | 0x20 TLM | 0x30 EID
	Namespace    string
	Instance     string
	URL          string
	TxPower      *int8
	BatteryMV    *uint16
	TemperatureC *float64
	AdvCount     *uint32
	SecCount     *uint32
}

// Advert is the fully decoded advertisement.
type Advert struct {
	Raw         []byte
	Structures  []ADStructure
	Flags       *byte
	LocalName   string // complete (0x09) preferred over shortened (0x08)
	TxPower     *int8  // advertised tx power (AD 0x0A)
	Services16  []uint16
	Services128 []string
	Mfg         []MfgRecord
	ServiceData []SvcDataRecord
	IBeacon     *IBeaconFrame
	Eddystone   *EddystoneFrame
}

const (
	adFlags        = 0x01
	adSvc16Inc     = 0x02
	adSvc16Comp    = 0x03
	adSvc128Inc    = 0x06
	adSvc128Comp   = 0x07
	adNameShort    = 0x08
	adNameComplete = 0x09
	adTxPower      = 0x0A
	adServiceData  = 0x16
	// manufDataType (0xFF) is defined in advert.go.

	eddystoneUUID16 = 0xFEAA
)

// Parse fully walks the hex-encoded advertisement. A malformed hex
// string or a TLV length that runs past the buffer is a hard error;
// individual records that are too short for their specialized decoder
// are recorded in Structures but skip specialization.
func Parse(hexStr string) (Advert, error) {
	var a Advert
	if hexStr == "" {
		return a, nil
	}
	buf, err := hex.DecodeString(hexStr)
	if err != nil {
		return a, fmt.Errorf("hex decode: %w", err)
	}
	a.Raw = buf

	for i := 0; i < len(buf); {
		recLen := int(buf[i])
		if recLen == 0 {
			break // zero length terminates the AD list (BLE spec)
		}
		if i+1+recLen > len(buf) {
			return a, fmt.Errorf("TLV at offset %d: length %d runs past end of %d-byte buffer", i, recLen, len(buf))
		}
		recType := buf[i+1]
		value := buf[i+2 : i+1+recLen]
		a.Structures = append(a.Structures, ADStructure{Type: recType, Value: append([]byte(nil), value...)})
		a.decodeStructure(recType, value)
		i += 1 + recLen
	}
	return a, nil
}

func (a *Advert) decodeStructure(t byte, v []byte) {
	switch t {
	case adFlags:
		if len(v) >= 1 {
			f := v[0]
			a.Flags = &f
		}
	case adNameComplete:
		a.LocalName = string(v) // complete always wins
	case adNameShort:
		if a.LocalName == "" {
			a.LocalName = string(v)
		}
	case adTxPower:
		if len(v) >= 1 {
			tx := int8(v[0])
			a.TxPower = &tx
		}
	case adSvc16Inc, adSvc16Comp:
		for j := 0; j+2 <= len(v); j += 2 {
			a.Services16 = append(a.Services16, binary.LittleEndian.Uint16(v[j:j+2]))
		}
	case adSvc128Inc, adSvc128Comp:
		for j := 0; j+16 <= len(v); j += 16 {
			a.Services128 = append(a.Services128, hex.EncodeToString(reversed(v[j:j+16])))
		}
	case manufDataType:
		if len(v) >= 2 {
			rec := MfgRecord{CompanyID: binary.LittleEndian.Uint16(v[:2]), Payload: append([]byte(nil), v[2:]...)}
			a.Mfg = append(a.Mfg, rec)
			if looksLikeIBeacon(v) {
				if ib, err := decodeIBeaconPayload(v); err == nil {
					f := IBeaconFrame{UUID: ib.UUID, Major: ib.Major, Minor: ib.Minor, TxPower: ib.TxPower}
					a.IBeacon = &f
				}
			}
		}
	case adServiceData:
		if len(v) >= 2 {
			uuid := binary.LittleEndian.Uint16(v[:2])
			rec := SvcDataRecord{UUID16: uuid, Payload: append([]byte(nil), v[2:]...)}
			a.ServiceData = append(a.ServiceData, rec)
			if uuid == eddystoneUUID16 {
				if ed := decodeEddystone(v[2:]); ed != nil {
					a.Eddystone = ed
				}
			}
		}
	}
}

func reversed(b []byte) []byte {
	out := make([]byte, len(b))
	for i := range b {
		out[i] = b[len(b)-1-i]
	}
	return out
}

// decodeEddystone decodes the Eddystone service-data payload (the bytes
// following the 0xFEAA UUID). Returns nil if too short to classify.
func decodeEddystone(p []byte) *EddystoneFrame {
	if len(p) < 1 {
		return nil
	}
	ed := &EddystoneFrame{FrameType: p[0]}
	switch p[0] {
	case 0x00: // UID: type(1) txpower(1) namespace(10) instance(6) [rfu(2)]
		if len(p) < 18 {
			return nil
		}
		tx := int8(p[1])
		ed.TxPower = &tx
		ed.Namespace = hex.EncodeToString(p[2:12])
		ed.Instance = hex.EncodeToString(p[12:18])
	case 0x10: // URL: type(1) txpower(1) scheme(1) encoded-url(...)
		if len(p) < 3 {
			return nil
		}
		tx := int8(p[1])
		ed.TxPower = &tx
		ed.URL = decodeEddystoneURL(p[2], p[3:])
	case 0x20: // TLM: type(1) version(1) batt(2) temp(2) advcnt(4) seccnt(4)
		if len(p) < 14 {
			return nil
		}
		batt := binary.BigEndian.Uint16(p[2:4])
		ed.BatteryMV = &batt
		// Temperature is 8.8 fixed-point signed; 0x8000 is the Eddystone-TLM
		// "not supported / not available" sentinel — leave nil rather than
		// publishing a bogus -128.0 °C.
		rawTemp := int16(binary.BigEndian.Uint16(p[4:6]))
		if rawTemp != int16(-0x8000) {
			temp := float64(rawTemp) / 256.0
			ed.TemperatureC = &temp
		}
		adv := binary.BigEndian.Uint32(p[6:10])
		ed.AdvCount = &adv
		sec := binary.BigEndian.Uint32(p[10:14])
		ed.SecCount = &sec
	case 0x30: // EID: ephemeral; no stable identity
	}
	return ed
}

var eddystoneURLSchemes = []string{"http://www.", "https://www.", "http://", "https://"}

var eddystoneURLExpansions = map[byte]string{
	0x00: ".com/", 0x01: ".org/", 0x02: ".edu/", 0x03: ".net/", 0x04: ".info/",
	0x05: ".biz/", 0x06: ".gov/", 0x07: ".com", 0x08: ".org", 0x09: ".edu",
	0x0a: ".net", 0x0b: ".info", 0x0c: ".biz", 0x0d: ".gov",
}

func decodeEddystoneURL(scheme byte, encoded []byte) string {
	var b strings.Builder
	if int(scheme) < len(eddystoneURLSchemes) {
		b.WriteString(eddystoneURLSchemes[scheme])
	}
	for _, c := range encoded {
		if exp, ok := eddystoneURLExpansions[c]; ok {
			b.WriteString(exp)
		} else if c >= 0x20 && c < 0x7f {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// mfgAllowlist gates which manufacturer company ids may yield an mfg
// identity. Empty by default: mfg identity is opt-in (ADR-0008) because
// most vendor payloads embed rotating bytes. Populate (with a stable
// payload slicer) when a real fixed-payload device is observed.
var mfgAllowlist = map[uint16]bool{}

// Identify derives the packet-derived identity by precedence:
//
//	ibeacon > eddystone_uid > eddystone_url > mfg(allowlisted) > name > anonymous
//
// ok=false means the advert is anonymous/ephemeral (rotating Apple
// Continuity, Eddystone-TLM/URL with no resolvable id, EID): count it,
// don't store it. euid is deliberately NOT an input — identity is
// packet-derived only (ADR-0008).
func Identify(a Advert) (ids.BeaconKey, bool) {
	if a.IBeacon != nil {
		if k, err := ids.NewIBeaconKey(a.IBeacon.UUID, a.IBeacon.Major, a.IBeacon.Minor); err == nil {
			return k, true
		}
	}
	if a.Eddystone != nil {
		switch a.Eddystone.FrameType {
		case 0x00:
			if k, err := ids.NewEddystoneUIDKey(a.Eddystone.Namespace, a.Eddystone.Instance); err == nil {
				return k, true
			}
		case 0x10:
			if a.Eddystone.URL != "" {
				if k, err := ids.NewEddystoneURLKey(a.Eddystone.URL); err == nil {
					return k, true
				}
			}
		}
	}
	for _, m := range a.Mfg {
		if mfgAllowlist[m.CompanyID] {
			if k, err := ids.NewMfgKey(m.CompanyID, m.Payload); err == nil {
				return k, true
			}
		}
	}
	if a.LocalName != "" {
		if k, err := ids.NewNameKey(a.LocalName); err == nil {
			return k, true
		}
	}
	return ids.BeaconKey{}, false
}
